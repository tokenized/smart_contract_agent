package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/cacher"

	"github.com/pkg/errors"
)

const (
	ballotVersion = uint8(0)
	ballotPath    = "ballots"
)

type BallotCache struct {
	cacher cacher.Cacher
	typ    reflect.Type
}

type Ballot struct {
	LockingScript bitcoin.Script  `bsor:"1" json:"locking_script"`
	Quantity      uint64          `bsor:"2" json:"quantity"`
	TxID          *bitcoin.Hash32 `bsor:"3" json:"txid,omitempty"`
	Vote          string          `bsor:"4" json:"vote"`

	markModified atomic.Value
	isModified   atomic.Value
	sync.Mutex   `bsor:"-"`
}

type Ballots map[bitcoin.Hash32]*Ballot

func NewBallotCache(cache cacher.Cacher) (*BallotCache, error) {
	typ := reflect.TypeOf(&Ballot{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.SetValue); !ok {
		return nil, errors.New("Type must implement CacheSetValue")
	}

	return &BallotCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *BallotCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, ballot *Ballot) (*Ballot, error) {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)

	value, err := c.cacher.AddSetValue(ctx, c.typ, pathPrefix, ballot)
	if err != nil {
		return nil, errors.Wrap(err, "add set")
	}

	return value.(*Ballot), nil
}

func (c *BallotCache) AddMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, ballots Ballots) (Ballots, error) {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)

	values := make([]cacher.SetValue, len(ballots))
	i := 0
	for _, ballot := range ballots {
		values[i] = ballot
		i++
	}

	addedValues, err := c.cacher.AddMultiSetValue(ctx, c.typ, pathPrefix, values)
	if err != nil {
		return nil, errors.Wrap(err, "add set multi")
	}

	result := make(Ballots)
	for _, value := range addedValues {
		ballot := value.(*Ballot)
		hash := LockingScriptHash(ballot.LockingScript)
		result[hash] = ballot
	}

	return result, nil
}

func (c *BallotCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, lockingScript bitcoin.Script) (*Ballot, error) {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)
	hash := LockingScriptHash(lockingScript)

	value, err := c.cacher.GetSetValue(ctx, c.typ, pathPrefix, hash)
	if err != nil {
		return nil, errors.Wrap(err, "get set")
	}

	if value == nil {
		return nil, nil
	}

	return value.(*Ballot), nil
}

func (c *BallotCache) GetMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, lockingScripts []bitcoin.Script) (Ballots, error) {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)
	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		hashes[i] = LockingScriptHash(lockingScript)
	}

	values, err := c.cacher.GetMultiSetValue(ctx, c.typ, pathPrefix, hashes)
	if err != nil {
		return nil, errors.Wrap(err, "get set multi")
	}

	result := make(Ballots, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}

		ballot := value.(*Ballot)
		hash := LockingScriptHash(ballot.LockingScript)
		result[hash] = ballot
	}

	return result, nil
}

func (c *BallotCache) List(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) (Ballots, error) {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)
	values, err := c.cacher.ListMultiSetValue(ctx, c.typ, pathPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "list set multi")
	}

	result := make(Ballots, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}

		ballot := value.(*Ballot)
		hash := LockingScriptHash(ballot.LockingScript)
		result[hash] = ballot
	}

	return result, nil
}

func (c *BallotCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, ballot *Ballot) error {

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)
	ballot.Lock()
	hash := LockingScriptHash(ballot.LockingScript)
	ballot.Unlock()

	if err := c.cacher.ReleaseSetValue(ctx, c.typ, pathPrefix, hash); err != nil {
		return errors.Wrap(err, "release set")
	}

	return nil
}

func (c *BallotCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, ballots Ballots) error {

	if len(ballots) == 0 {
		return nil
	}

	pathPrefix := ballotPathPrefix(contractLockingScript, voteTxID)
	var hashes []bitcoin.Hash32
	for hash, ballot := range ballots {
		if ballot == nil {
			continue
		}

		ballot.Lock()
		hashes = append(hashes, hash)
		ballot.Unlock()
	}

	if err := c.cacher.ReleaseMultiSetValue(ctx, c.typ, pathPrefix, hashes); err != nil {
		return errors.Wrap(err, "release set multi")
	}

	return nil
}

func ballotPathPrefix(contractLockingScript bitcoin.Script, voteTxID bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s/%s", CalculateContractHash(contractLockingScript), voteTxID,
		ballotPath)
}

func (b *Ballot) Initialize() {
	b.isModified.Store(false)
}

func (b *Ballot) ProvideMarkModified(markModified cacher.MarkModified) {
	b.markModified.Store(markModified)
}

func (b *Ballot) MarkModified() {
	if v := b.markModified.Load(); v != nil {
		v.(cacher.MarkModified)()
	}

	b.isModified.Store(true)
}

func (b *Ballot) GetModified() bool {
	if v := b.isModified.Swap(false); v != nil {
		return v.(bool)
	}

	return false
}

func (b *Ballot) IsModified() bool {
	if v := b.isModified.Load(); v != nil {
		return v.(bool)
	}

	return false
}

func (b *Ballot) Hash() bitcoin.Hash32 {
	return LockingScriptHash(b.LockingScript)
}

func (b *Ballot) CacheSetCopy() cacher.SetValue {
	result := &Ballot{
		Quantity: b.Quantity,
		Vote:     b.Vote,
	}
	result.isModified.Store(true)

	result.LockingScript = make(bitcoin.Script, len(b.LockingScript))
	copy(result.LockingScript, b.LockingScript)

	if b.TxID != nil {
		result.TxID = &bitcoin.Hash32{}
		copy(result.TxID[:], b.TxID[:])
	}

	return result
}

func (b *Ballot) Serialize(w io.Writer) error {
	bs, err := bsor.MarshalBinary(b)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, ballotVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(bs))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(bs); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (b *Ballot) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	bs := make([]byte, size)
	if _, err := io.ReadFull(r, bs); err != nil {
		return errors.Wrap(err, "read")
	}

	b.Lock()
	defer b.Unlock()
	if _, err := bsor.UnmarshalBinary(bs, b); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
