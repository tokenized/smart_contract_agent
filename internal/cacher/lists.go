package cacher

import (
	"context"
	"io"
	"reflect"
	"sync"
)

// CacheListValue represents a value that is stored in a list with many other values in one storage
// object (s3 object, file). To use in a cache the entire section of the list must be treated as an
// object, but these "List" functions abstract that so individual values can be dealt with.
type CacheListValue interface {
	// Storage path and serialization.
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error

	// All functions will be called by cache while the object is locked.
	Lock()
	Unlock()
}

// cacheList represents a list of values that are all stored in one storage object (s3 object, file).
type cacheList struct {
	typ reflect.Type

	pathPrefix string
	offset     uint64

	values []CacheListValue

	isModified bool
	sync.Mutex
}

func (c *Cache) AddListValue(ctx context.Context, typ reflect.Type, pathPrefix string,
	value CacheListValue) error {

	// hash := value.Hash()
	// pathID := hashPathID(hash)
	// set := &cacheSet{
	// 	typ:        typ,
	// 	pathPrefix: pathPrefix,
	// 	pathID:     pathID,
	// 	values:     make(map[bitcoin.Hash32]CacheSetValue),
	// }
	// set.values[hash] = value

	// item, err := c.Add(ctx, c.cacheSetType, set)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "add")
	// }
	// set = item.(*cacheSet)

	// set.Lock()
	// result, exists := set.values[hash]

	// if exists {
	// 	// Value already exists so return existing value to be updated.
	// 	set.Unlock()
	// 	return result, nil
	// }

	// // Add value to set
	// set.values[hash] = value
	// set.MarkModified()
	// set.Unlock()

	// return value, nil
	return nil
}
