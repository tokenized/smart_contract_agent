package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	ci "github.com/tokenized/pkg/cacher"

	"github.com/pkg/errors"
)

const (
	SubscriptionTypeInvalid       = SubscriptionType(0)
	SubscriptionTypeLockingScript = SubscriptionType(1)

	subscriptionVersion = uint8(0)
	subscriptionPath    = "subscriptions"
)

type SubscriptionCache struct {
	cacher ci.Cacher
	typ    reflect.Type
}

type SubscriptionType uint8

type Subscription struct {
	value SubscriptionValue
}

type Subscriptions []*Subscription

type SubscriptionValue interface {
	GetChannelHash() bitcoin.Hash32

	// Object has been modified since last write to storage.
	IsModified() bool
	MarkModified()
	ClearModified()

	// Storage path and serialization.
	Hash() bitcoin.Hash32
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error

	CacheSetCopy() ci.SetValue

	// All functions will be called by cache while the object is locked.
	Lock()
	Unlock()
}

type LockingScriptSubscription struct {
	LockingScript bitcoin.Script `bsor:"1" json:"locking_script"`
	ChannelHash   bitcoin.Hash32 `bsor:"2" json:"channel_hash"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func NewSubscriptionCache(cache ci.Cacher) (*SubscriptionCache, error) {
	typ := reflect.TypeOf(&Subscription{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must be support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(ci.SetValue); !ok {
		return nil, errors.New("Type must implement CacheSetValue")
	}

	return &SubscriptionCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *SubscriptionCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	subscription *Subscription) (*Subscription, error) {

	pathPrefix := subscriptionPathPrefix(contractLockingScript)

	value, err := c.cacher.AddSetValue(ctx, c.typ, pathPrefix, subscription)
	if err != nil {
		return nil, errors.Wrap(err, "add set")
	}

	return value.(*Subscription), nil
}

func (c *SubscriptionCache) AddMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	subscriptions Subscriptions) (Subscriptions, error) {

	pathPrefix := subscriptionPathPrefix(contractLockingScript)

	values := make([]ci.SetValue, len(subscriptions))
	for i, subscription := range subscriptions {
		values[i] = subscription
	}

	addedValues, err := c.cacher.AddMultiSetValue(ctx, c.typ, pathPrefix, values)
	if err != nil {
		return nil, errors.Wrap(err, "add set multi")
	}

	result := make(Subscriptions, len(addedValues))
	for i, value := range addedValues {
		result[i] = value.(*Subscription)
	}

	return result, nil
}

func (c *SubscriptionCache) GetLockingScript(ctx context.Context,
	contractLockingScript bitcoin.Script, lockingScript bitcoin.Script) (*Subscription, error) {

	pathPrefix := subscriptionPathPrefix(contractLockingScript)
	hash := LockingScriptHash(lockingScript)

	value, err := c.cacher.GetSetValue(ctx, c.typ, pathPrefix, hash)
	if err != nil {
		return nil, errors.Wrap(err, "get set")
	}

	if value == nil {
		return nil, nil
	}

	return value.(*Subscription), nil
}

func (c *SubscriptionCache) GetLockingScriptMulti(ctx context.Context,
	contractLockingScript bitcoin.Script,
	lockingScripts []bitcoin.Script) (Subscriptions, error) {

	pathPrefix := subscriptionPathPrefix(contractLockingScript)
	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		hashes[i] = LockingScriptHash(lockingScript)
	}

	values, err := c.cacher.GetMultiSetValue(ctx, c.typ, pathPrefix, hashes)
	if err != nil {
		return nil, errors.Wrap(err, "get set multi")
	}

	result := make(Subscriptions, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}

		result[i] = value.(*Subscription)
	}

	return result, nil
}

func (c *SubscriptionCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	subscription *Subscription) error {

	pathPrefix := subscriptionPathPrefix(contractLockingScript)
	subscription.Lock()
	hash := subscription.Hash()
	isModified := subscription.IsModified()
	subscription.Unlock()

	if err := c.cacher.ReleaseSetValue(ctx, c.typ, pathPrefix, hash, isModified); err != nil {
		return errors.Wrap(err, "release set")
	}

	return nil
}

func (c *SubscriptionCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	subscriptions Subscriptions) error {

	if len(subscriptions) == 0 {
		return nil
	}

	pathPrefix := subscriptionPathPrefix(contractLockingScript)
	var hashes []bitcoin.Hash32
	var isModified []bool
	for _, subscription := range subscriptions {
		if subscription == nil {
			continue
		}

		subscription.Lock()
		hashes = append(hashes, subscription.Hash())
		isModified = append(isModified, subscription.IsModified())
		subscription.Unlock()
	}

	if err := c.cacher.ReleaseMultiSetValue(ctx, c.typ, pathPrefix, hashes,
		isModified); err != nil {
		return errors.Wrap(err, "release set multi")
	}

	return nil
}

func subscriptionPathPrefix(contractLockingScript bitcoin.Script) string {
	return fmt.Sprintf("%s/%s", CalculateContractHash(contractLockingScript), subscriptionPath)
}

func SubscriptionForType(typ SubscriptionType) SubscriptionValue {
	switch typ {
	case SubscriptionTypeLockingScript:
		return &LockingScriptSubscription{}
	default:
		return nil
	}
}

func (s *Subscription) Type() SubscriptionType {
	switch s.value.(type) {
	case *LockingScriptSubscription:
		return SubscriptionTypeLockingScript
	default:
		return 0
	}
}

func (s *Subscription) GetChannelHash() bitcoin.Hash32 {
	return s.value.GetChannelHash()
}

func (s *Subscription) IsModified() bool {
	return s.value.IsModified()
}

func (s *Subscription) ClearModified() {
	s.value.ClearModified()
}

func (s *Subscription) Hash() bitcoin.Hash32 {
	return s.value.Hash()
}

func (s *Subscription) CacheSetCopy() ci.SetValue {
	return s.value.CacheSetCopy()
}

func (s *Subscription) Serialize(w io.Writer) error {
	typ := s.Type()
	if typ == 0 {
		return errors.New("Unknown Subscription Type")
	}

	if err := binary.Write(w, endian, typ); err != nil {
		return errors.Wrap(err, "type")
	}

	return s.value.Serialize(w)
}

func (s *Subscription) Deserialize(r io.Reader) error {
	var typ SubscriptionType
	if err := binary.Read(r, endian, &typ); err != nil {
		return errors.Wrap(err, "type")
	}

	s.value = SubscriptionForType(typ)
	if s.value == nil {
		return fmt.Errorf("Unknown subscription type : %d %s", uint8(typ), typ)
	}

	if err := s.value.Deserialize(r); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func (s *Subscription) Lock() {
	s.value.Lock()
}

func (s *Subscription) Unlock() {
	s.value.Unlock()
}

func (s *LockingScriptSubscription) Hash() bitcoin.Hash32 {
	return LockingScriptHash(s.LockingScript)
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

func (s *LockingScriptSubscription) ClearModified() {
	s.isModified = false
}

func (s *LockingScriptSubscription) CacheSetCopy() ci.SetValue {
	result := &LockingScriptSubscription{
		LockingScript: make(bitcoin.Script, len(s.LockingScript)),
	}

	copy(result.LockingScript, s.LockingScript)
	copy(result.ChannelHash[:], s.ChannelHash[:])

	return result
}

func (s *LockingScriptSubscription) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(s)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (s *LockingScriptSubscription) Deserialize(r io.Reader) error {
	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "read")
	}

	if _, err := bsor.UnmarshalBinary(b, s); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
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
