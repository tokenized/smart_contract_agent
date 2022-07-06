package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/cacher"

	"github.com/pkg/errors"
)

const (
	SubscriptionTypeLockingScript = SubscriptionType(1)

	subscriptionVersion = uint8(0)
	subscriptionPath    = "subscriptions"
)

type SubscriptionCache struct {
	cacher *cacher.Cache
}

type subscriptionSet struct {
	PathID [2]byte `json:"path_id"`

	ContractLockingScript bitcoin.Script `json:"contract_locking_script"`

	Subscriptions map[bitcoin.Hash32]Subscription `json:"subscriptions"`

	isModified bool
	sync.Mutex
}

type subscriptionSets []*subscriptionSet

type SubscriptionType uint8

type Subscription interface {
	Type() SubscriptionType
	Hash() bitcoin.Hash32
	GetChannelHash() bitcoin.Hash32

	MarkModified()
	IsModified() bool

	Lock()
	Unlock()
}

type Subscriptions []Subscription

type LockingScriptSubscription struct {
	LockingScript bitcoin.Script `bsor:"1" json:"locking_script"`
	ChannelHash   bitcoin.Hash32 `bsor:"2" json:"channel_hash"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func (s *LockingScriptSubscription) Type() SubscriptionType {
	return SubscriptionTypeLockingScript
}

func (s *LockingScriptSubscription) Hash() bitcoin.Hash32 {
	return LockingScriptHash(s.LockingScript)
}

func LockingScriptHash(lockingScript bitcoin.Script) bitcoin.Hash32 {
	return bitcoin.Hash32(sha256.Sum256(lockingScript))
}

func (s *LockingScriptSubscription) GetChannelHash() bitcoin.Hash32 {
	return s.ChannelHash
}

func (s *LockingScriptSubscription) MarkModified() {
	s.isModified = true
}

func (s *LockingScriptSubscription) IsModified() bool {
	return s.isModified
}

func NewSubscriptionCache(store storage.StreamStorage, requestThreadCount int,
	requestTimeout time.Duration, expireCount int,
	expiration time.Duration) (*SubscriptionCache, error) {

	cacher, err := cacher.NewCache(store, reflect.TypeOf(&subscriptionSet{}), requestThreadCount,
		requestTimeout, expireCount, expiration)
	if err != nil {
		return nil, errors.Wrap(err, "cacher")
	}

	return &SubscriptionCache{
		cacher: cacher,
	}, nil
}

func (c *SubscriptionCache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	return c.cacher.Run(ctx, interrupt)
}

func (c *SubscriptionCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	subscription Subscription) (Subscription, error) {

	hash := subscription.Hash()
	pathID := hashPathID(hash)
	set := &subscriptionSet{
		PathID:                pathID,
		ContractLockingScript: contractLockingScript,
		Subscriptions:         make(map[bitcoin.Hash32]Subscription),
	}
	set.Subscriptions[hash] = subscription

	item, err := c.cacher.Add(ctx, set)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}
	set = item.(*subscriptionSet)

	set.Lock()
	result, exists := set.Subscriptions[hash]

	if exists {
		set.Unlock()
		return result, nil
	}

	set.Subscriptions[hash] = subscription
	set.MarkModified()
	set.Unlock()

	return subscription, nil
}

func (c *SubscriptionCache) AddMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	subscriptions Subscriptions) (Subscriptions, error) {

	var sets subscriptionSets
	for _, subscription := range subscriptions {
		sets.add(contractLockingScript, subscription)
	}

	values := make([]cacher.CacheValue, len(sets))
	for i, set := range sets {
		values[i] = set
	}

	items, err := c.cacher.AddMulti(ctx, values)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	// Convert from items to sets
	sets = make(subscriptionSets, len(items))
	for i, item := range items {
		sets[i] = item.(*subscriptionSet)
	}

	// Build resulting subscriptions from sets.
	result := make(Subscriptions, len(subscriptions))
	for i, subscription := range subscriptions {
		hash := subscription.Hash()
		set := sets.getSet(contractLockingScript, hash)
		if set == nil {
			// This shouldn't be possible if cacher.AddMulti is functioning properly.
			return nil, errors.New("Subscription Set Missing") // subscription set not within sets
		}

		set.Lock()
		gotSubscription, exists := set.Subscriptions[hash]
		if exists {
			result[i] = gotSubscription
		} else {
			result[i] = subscription
			set.Subscriptions[hash] = subscription
			set.isModified = true
		}
		set.Unlock()
	}

	return result, nil
}

func (c *SubscriptionCache) GetLockingScript(ctx context.Context,
	contractLockingScript bitcoin.Script, lockingScript bitcoin.Script) (Subscription, error) {

	hash := LockingScriptHash(lockingScript)
	path := subscriptionSetPath(contractLockingScript, hashPathID(hash))

	item, err := c.cacher.Get(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil // set doesn't exist
	}

	set := item.(*subscriptionSet)
	set.Lock()
	subscription, exists := set.Subscriptions[hash]
	set.Unlock()

	if !exists {
		c.cacher.Release(ctx, path)
		return nil, nil // subscription not within set
	}

	return subscription, nil
}

func (c *SubscriptionCache) GetLockingScriptMulti(ctx context.Context,
	contractLockingScript bitcoin.Script,
	lockingScripts []bitcoin.Script) (Subscriptions, error) {

	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	paths := make([]string, len(lockingScripts))
	var getPaths []string
	for i, lockingScript := range lockingScripts {
		hash := LockingScriptHash(lockingScript)
		path := subscriptionSetPath(contractLockingScript, hashPathID(hash))
		hashes[i] = hash
		paths[i] = path
		getPaths = appendStringIfDoesntExist(getPaths, path)
	}

	items, err := c.cacher.GetMulti(ctx, getPaths)
	if err != nil {
		return nil, errors.Wrap(err, "get multi")
	}

	sets := make(subscriptionSets, len(items))
	for i, item := range items {
		if item == nil {
			continue // a requested set must not exist
		}

		sets[i] = item.(*subscriptionSet)
	}

	result := make(Subscriptions, len(lockingScripts))
	for i := range lockingScripts {
		set := sets.getSet(contractLockingScript, hashes[i])
		if set == nil {
			c.cacher.Release(ctx, paths[i])
			continue
		}

		set.Lock()
		subscription, exists := set.Subscriptions[hashes[i]]
		set.Unlock()

		if !exists {
			c.cacher.Release(ctx, paths[i])
			continue // subscription not within set
		}

		result[i] = subscription
	}

	return result, nil
}

func (c *SubscriptionCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	subscription Subscription) error {

	subscription.Lock()
	path := subscriptionSetPath(contractLockingScript, hashPathID(subscription.Hash()))
	isModified := subscription.IsModified()
	subscription.Unlock()

	// Set set as modified
	if isModified {
		item, err := c.cacher.Get(ctx, path)
		if err != nil {
			return errors.Wrap(err, "get")
		}

		if item == nil {
			return errors.New("Subscription Set Missing")
		}

		set := item.(*subscriptionSet)
		set.Lock()
		set.isModified = true
		set.Unlock()

		c.cacher.Release(ctx, path) // release for get above
	}

	c.cacher.Release(ctx, path) // release for original request
	return nil
}

func (c *SubscriptionCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	subscriptions Subscriptions) error {

	var modifiedPaths, allPaths []string
	hashes := make([]bitcoin.Hash32, len(subscriptions))
	isModified := make([]bool, len(subscriptions))
	for i, subscription := range subscriptions {
		subscription.Lock()
		hashes[i] = subscription.Hash()
		path := subscriptionSetPath(contractLockingScript, hashPathID(hashes[i]))
		isModified[i] = subscription.IsModified()
		subscription.Unlock()

		allPaths = appendStringIfDoesntExist(allPaths, path)
		if isModified[i] {
			modifiedPaths = appendStringIfDoesntExist(modifiedPaths, path)
		}
	}

	modifiedItems, err := c.cacher.GetMulti(ctx, modifiedPaths)
	if err != nil {
		return errors.Wrap(err, "get multi")
	}

	sets := make(subscriptionSets, len(modifiedItems))
	for i, item := range modifiedItems {
		if item == nil {
			continue // a requested set must not exist
		}

		sets[i] = item.(*subscriptionSet)
	}

	for i := range subscriptions {
		if !isModified[i] {
			continue
		}

		set := sets.getSet(contractLockingScript, hashes[i])
		if set == nil {
			return errors.New("Subscription Set Missing")
		}

		set.MarkModified()
	}

	// Release from GetMulti above.
	for _, path := range modifiedPaths {
		c.cacher.Release(ctx, path)
	}

	// Release for subscriptions specified in this function call.
	for _, path := range allPaths {
		c.cacher.Release(ctx, path)
	}

	return nil
}

func (sets *subscriptionSets) add(contractLockingScript bitcoin.Script, subscription Subscription) {
	hash := subscription.Hash()
	pathID := hashPathID(hash)

	for _, set := range *sets {
		set.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.PathID[:], pathID[:]) {
			set.Unlock()
			continue
		}

		set.Subscriptions[hash] = subscription
		set.Unlock()
		return
	}

	set := &subscriptionSet{
		PathID:                pathID,
		ContractLockingScript: contractLockingScript,
		Subscriptions:         make(map[bitcoin.Hash32]Subscription),
	}

	set.Lock()
	set.Subscriptions[hash] = subscription
	set.Unlock()
	*sets = append(*sets, set)
}

func (sets *subscriptionSets) getSet(contractLockingScript bitcoin.Script,
	hash bitcoin.Hash32) *subscriptionSet {

	pathID := hashPathID(hash)
	for _, set := range *sets {
		set.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.PathID[:], pathID[:]) {
			set.Unlock()
			continue
		}

		set.Unlock()
		return set
	}

	return nil
}

func subscriptionSetPath(contractLockingScript bitcoin.Script, pathID [2]byte) string {
	return fmt.Sprintf("%s/%s/%x", subscriptionPath,
		CalculateContractID(contractLockingScript), pathID)
}

func (set *subscriptionSet) Path() string {
	return fmt.Sprintf("%s/%s/%x", subscriptionPath,
		CalculateContractID(set.ContractLockingScript), set.PathID)
}

func (set *subscriptionSet) MarkModified() {
	set.isModified = true
}

func (set *subscriptionSet) ClearModified() {
	set.isModified = false
}

func (set *subscriptionSet) IsModified() bool {
	return set.isModified
}

func (set *subscriptionSet) Serialize(w io.Writer) error {
	if err := binary.Write(w, endian, subscriptionVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if _, err := w.Write(set.PathID[:]); err != nil {
		return errors.Wrap(err, "path_id")
	}

	if err := writeString(w, set.ContractLockingScript); err != nil {
		return errors.Wrap(err, "contract")
	}

	if err := binary.Write(w, endian, uint32(len(set.Subscriptions))); err != nil {
		return errors.Wrap(err, "size")
	}

	for _, subscription := range set.Subscriptions {
		if err := binary.Write(w, endian, subscription.Type()); err != nil {
			return errors.Wrap(err, "type")
		}

		b, err := bsor.MarshalBinary(subscription)
		if err != nil {
			return errors.Wrap(err, "marshal")
		}

		if err := binary.Write(w, endian, uint32(len(b))); err != nil {
			return errors.Wrap(err, "size")
		}

		if _, err := w.Write(b); err != nil {
			return errors.Wrap(err, "write")
		}
	}

	return nil
}

func (set *subscriptionSet) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	if _, err := io.ReadFull(r, set.PathID[:]); err != nil {
		return errors.Wrap(err, "path_id")
	}

	contractLockingScript, err := readString(r)
	if err != nil {
		return errors.Wrap(err, "contract")
	}
	set.ContractLockingScript = contractLockingScript

	var count uint32
	if err := binary.Read(r, endian, &count); err != nil {
		return errors.Wrap(err, "count")
	}

	set.Subscriptions = make(map[bitcoin.Hash32]Subscription)
	for i := uint32(0); i < count; i++ {
		var size uint32
		if err := binary.Read(r, endian, &size); err != nil {
			return errors.Wrap(err, "size")
		}

		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			return errors.Wrap(err, "read")
		}

		subscription, err := DeserializeSubscription(r)
		if err != nil {
			return errors.Wrap(err, "subscription")
		}

		hash := subscription.Hash()
		set.Subscriptions[hash] = subscription
	}

	return nil
}

func DeserializeSubscription(r io.Reader) (Subscription, error) {
	var typ [1]byte
	if _, err := io.ReadFull(r, typ[:]); err != nil {
		return nil, errors.Wrap(err, "type")
	}

	var subscription Subscription
	switch SubscriptionType(typ[0]) {
	case SubscriptionTypeLockingScript:
		subscription = &LockingScriptSubscription{}
	default:
		return nil, fmt.Errorf("Unknown subscription type %d : %s", typ, SubscriptionType(typ[0]))
	}

	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return nil, errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "read")
	}

	if _, err := bsor.UnmarshalBinary(b, subscription); err != nil {
		return nil, errors.Wrap(err, "unmarshal")
	}

	return subscription, nil
}

func (v SubscriptionType) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown SubscriptionType value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *SubscriptionType) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *SubscriptionType) SetString(s string) error {
	switch s {
	case "locking_script":
		*v = SubscriptionTypeLockingScript
	default:
		*v = 0
		return fmt.Errorf("Unknown SubscriptionType value \"%s\"", s)
	}

	return nil
}

func (v SubscriptionType) String() string {
	switch v {
	case SubscriptionTypeLockingScript:
		return "locking_script"
	default:
		return "unknown"
	}
}
