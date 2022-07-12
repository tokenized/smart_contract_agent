package cacher

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/pkg/bitcoin"

	"github.com/pkg/errors"
)

const (
	cacheSetVersion = uint8(0)
)

var (
	endian = binary.LittleEndian
)

// CacheSetValue represents a value that is stored in a set with many other values in one storage
// object (s3 object, file). To use in a cache the entire set must be treated as an object, but
// these "Set" functions abstract that so individual values can be dealt with.
type CacheSetValue interface {
	// Object has been modified since last write to storage.
	IsModified() bool
	ClearModified()

	// Storage path and serialization.
	Hash() bitcoin.Hash32
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error

	// All functions will be called by cache while the object is locked.
	Lock()
	Unlock()
}

// cacheSet represents a set of values that are all stored in one storage object (s3 object, file).
type cacheSet struct {
	typ reflect.Type

	pathPrefix string
	pathID     [2]byte

	values map[bitcoin.Hash32]CacheSetValue

	isModified bool
	sync.Mutex
}

type cacheSets []*cacheSet

func (c *Cache) AddSetValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	value CacheSetValue) (CacheSetValue, error) {

	hash := value.Hash()
	pathID := hashPathID(hash)
	set := &cacheSet{
		typ:        typ,
		pathPrefix: pathPrefix,
		pathID:     pathID,
		values:     make(map[bitcoin.Hash32]CacheSetValue),
	}
	set.values[hash] = value

	item, err := c.Add(ctx, c.cacheSetType, set)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}
	set = item.(*cacheSet)

	set.Lock()
	set.pathPrefix = pathPrefix
	set.pathID = pathID

	result, exists := set.values[hash]

	if exists {
		// Value already exists so return existing value to be updated.
		set.Unlock()
		return result, nil
	}

	// Add value to set
	set.values[hash] = value
	set.MarkModified()
	set.Unlock()

	return value, nil
}

func (c *Cache) AddSetMultiValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	values []CacheSetValue) ([]CacheSetValue, error) {

	var sets cacheSets
	for _, value := range values {
		sets.add(pathPrefix, value)
	}

	cacheValues := make([]CacheValue, len(sets))
	for i, set := range sets {
		cacheValues[i] = set
	}

	items, err := c.AddMulti(ctx, c.cacheSetType, cacheValues)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	// Convert from items to sets
	sets = make(cacheSets, len(items))
	for i, item := range items {
		sets[i] = item.(*cacheSet)
	}

	// Build resulting values from sets.
	result := make([]CacheSetValue, len(values))
	for i, value := range values {
		hash := value.Hash()
		pathID := hashPathID(hash)
		set := sets.getSet(pathID)
		if set == nil {
			// This shouldn't be possible if cacher.AddMulti is functioning properly.
			return nil, errors.New("Value Set Missing") // value set not within sets
		}

		set.Lock()
		set.pathPrefix = pathPrefix
		set.pathID = pathID

		gotValue, exists := set.values[hash]
		if exists {
			// Value exists so return existing value to be modified.
			result[i] = gotValue
		} else {
			// Add new value to set.
			result[i] = value
			set.values[hash] = value
			set.isModified = true
		}
		set.Unlock()
	}

	return result, nil
}

func (c *Cache) GetSetValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	hash bitcoin.Hash32) (CacheSetValue, error) {

	pathID := hashPathID(hash)
	path := setPath(pathPrefix, pathID)

	item, err := c.Get(ctx, c.cacheSetType, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil // set doesn't exist
	}

	set := item.(*cacheSet)
	set.Lock()
	set.pathPrefix = pathPrefix
	set.pathID = pathID

	value, exists := set.values[hash]
	set.Unlock()

	if !exists {
		c.Release(ctx, path)
		return nil, nil // value not within set
	}

	return value, nil
}

func (c *Cache) GetSetMultiValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	hashes []bitcoin.Hash32) ([]CacheSetValue, error) {

	pathIDs := make([][2]byte, len(hashes))
	paths := make([]string, len(hashes))
	var getPaths []string
	for i, hash := range hashes {
		pathID := hashPathID(hash)
		pathIDs[i] = pathID
		path := setPath(pathPrefix, pathID)
		paths[i] = path
		getPaths = appendStringIfDoesntExist(getPaths, path)
	}

	items, err := c.GetMulti(ctx, c.cacheSetType, getPaths)
	if err != nil {
		return nil, errors.Wrap(err, "get multi")
	}

	var sets cacheSets
	for _, item := range items {
		if item == nil {
			continue // a requested set must not exist
		}

		sets = append(sets, item.(*cacheSet))
	}

	result := make([]CacheSetValue, len(hashes))
	for i, hash := range hashes {
		set := sets.getSet(pathIDs[i])
		if set == nil {
			continue
		}

		set.Lock()
		set.pathPrefix = pathPrefix
		set.pathID = pathIDs[i]

		value, exists := set.values[hash]
		set.Unlock()

		if !exists {
			c.Release(ctx, setPath(pathPrefix, pathIDs[i]))
			continue // value not within set
		}

		result[i] = value
	}

	return result, nil
}

func (c *Cache) ReleaseSetValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	hash bitcoin.Hash32, isModified bool) error {

	pathID := hashPathID(hash)
	path := setPath(pathPrefix, pathID)

	// Set set as modified
	if isModified {
		item, err := c.Get(ctx, c.cacheSetType, path)
		if err != nil {
			return errors.Wrap(err, "get")
		}

		if item == nil {
			return errors.New("Value Set Missing")
		}

		set := item.(*cacheSet)
		set.Lock()
		set.pathPrefix = pathPrefix
		set.pathID = pathID

		set.isModified = true
		set.Unlock()

		c.Release(ctx, path) // release for get above
	}

	c.Release(ctx, path) // release for original request
	return nil
}

func (c *Cache) ReleaseSetMultiValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	hashes []bitcoin.Hash32, isModified []bool) error {

	var modifiedPaths, allPaths []string
	for i, hash := range hashes {
		path := setPath(pathPrefix, hashPathID(hash))

		allPaths = appendStringIfDoesntExist(allPaths, path)
		if isModified[i] {
			modifiedPaths = appendStringIfDoesntExist(modifiedPaths, path)
		}
	}

	modifiedItems, err := c.GetMulti(ctx, c.cacheSetType, modifiedPaths)
	if err != nil {
		return errors.Wrap(err, "get multi")
	}

	var sets cacheSets
	for _, item := range modifiedItems {
		if item == nil {
			continue // a requested set must not exist
		}

		sets = append(sets, item.(*cacheSet))
	}

	for i, hash := range hashes {
		if !isModified[i] {
			continue
		}

		pathID := hashPathID(hash)
		set := sets.getSet(pathID)
		if set == nil {
			return errors.New("Balance Set Missing")
		}

		set.Lock()
		set.pathPrefix = pathPrefix
		set.pathID = pathID

		set.isModified = true
		set.Unlock()
	}

	// Release from GetMulti above.
	for _, path := range modifiedPaths {
		c.Release(ctx, path)
	}

	// Release for balances specified in this function call.
	for _, path := range allPaths {
		c.Release(ctx, path)
	}

	return nil
}

func (set *cacheSet) IsModified() bool {
	return set.isModified
}

func (set *cacheSet) ClearModified() {
	set.isModified = false
}

func (set *cacheSet) MarkModified() {
	set.isModified = true
}

func setPath(pathPrefix string, pathID [2]byte) string {
	return fmt.Sprintf("%s/%x", pathPrefix, pathID)
}

func (set *cacheSet) Path() string {
	return fmt.Sprintf("%s/%x", set.pathPrefix, set.pathID)
}

func (set *cacheSet) Serialize(w io.Writer) error {
	if err := binary.Write(w, endian, cacheSetVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint64(len(set.values))); err != nil {
		return errors.Wrap(err, "size")
	}

	for _, value := range set.values {
		if err := value.Serialize(w); err != nil {
			return errors.Wrap(err, "value")
		}
	}

	return nil
}

func (set *cacheSet) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var count uint64
	if err := binary.Read(r, endian, &count); err != nil {
		return errors.Wrap(err, "count")
	}

	set.values = make(map[bitcoin.Hash32]CacheSetValue)
	for i := uint64(0); i < count; i++ {
		itemValue := reflect.New(set.typ.Elem())
		valueInterface := itemValue.Interface()
		value := valueInterface.(CacheSetValue)

		if err := value.Deserialize(r); err != nil {
			return errors.Wrap(err, "value")
		}

		hash := value.Hash()
		set.values[hash] = value
	}

	return nil
}

func (sets *cacheSets) add(pathPrefix string, value CacheSetValue) {
	hash := value.Hash()
	pathID := hashPathID(hash)

	for _, set := range *sets {
		set.Lock()
		if pathPrefix != set.pathPrefix {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.pathID[:], pathID[:]) {
			set.Unlock()
			continue
		}

		set.values[hash] = value
		set.Unlock()
		return
	}

	set := &cacheSet{
		pathPrefix: pathPrefix,
		pathID:     pathID,
		values:     make(map[bitcoin.Hash32]CacheSetValue),
	}

	set.Lock()
	set.values[hash] = value
	set.Unlock()
	*sets = append(*sets, set)
}

func (sets *cacheSets) getSet(pathID [2]byte) *cacheSet {
	for _, set := range *sets {
		set.Lock()
		if !bytes.Equal(set.pathID[:], pathID[:]) {
			set.Unlock()
			continue
		}

		set.Unlock()
		return set
	}

	return nil
}

func hashPathID(hash bitcoin.Hash32) [2]byte {
	var pathID [2]byte
	copy(pathID[:], hash[:])
	return pathID
}
