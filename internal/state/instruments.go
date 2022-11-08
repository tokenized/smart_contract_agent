package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"

	"github.com/pkg/errors"
)

const (
	instrumentVersion = uint8(0)
	instrumentPath    = "instruments"
)

type InstrumentCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

// Instrument represents the intruments created under a contract and are stored with the contract.
type Instrument struct {
	InstrumentType [3]byte                     `bsor:"1" json:"instrument_type"`
	InstrumentCode InstrumentCode              `bsor:"2" json:"instrument_id"`
	Creation       *actions.InstrumentCreation `bsor:"3" json:"creation`
	CreationTxID   *bitcoin.Hash32             `bsor:"4" json:"creation_txid"`

	FrozenUntil *uint64 `bsor:"5" json:"frozen_until,omitempty"`

	// payload is used to cache the deserialized value of the payload in Creation.
	payload instruments.Instrument `json:"instrument"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func NewInstrumentCache(cache *cacher.Cache) (*InstrumentCache, error) {
	typ := reflect.TypeOf(&Instrument{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.CacheValue); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &InstrumentCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *InstrumentCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	instrument *Instrument) (*Instrument, error) {

	instrument.Lock()
	instrumentCode := instrument.InstrumentCode
	instrument.Unlock()

	path := InstrumentPath(CalculateContractHash(contractLockingScript), instrumentCode)

	item, err := c.cacher.Add(ctx, c.typ, path, instrument)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Instrument), nil
}

func (c *InstrumentCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode) (*Instrument, error) {

	path := InstrumentPath(CalculateContractHash(contractLockingScript), instrumentCode)

	item, err := c.cacher.Get(ctx, c.typ, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Instrument), nil
}

func (c *InstrumentCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode) {
	c.cacher.Release(ctx, InstrumentPath(CalculateContractHash(contractLockingScript),
		instrumentCode))
}

func (i *Instrument) ClearInstrument() {
	i.payload = nil
}

func (i *Instrument) GetPayload() (instruments.Instrument, error) {
	if i.payload != nil {
		return i.payload, nil
	}

	// Decode payload
	payload, err := instruments.Deserialize(i.InstrumentType[:], i.Creation.InstrumentPayload)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize")
	}

	i.payload = payload
	return i.payload, nil
}

func (i *Instrument) TransfersPermitted() bool {
	payload, err := i.GetPayload()
	if err != nil {
		return false
	}

	switch pl := payload.(type) {
	case *instruments.Currency:
		return true
	case *instruments.Membership:
		return pl.TransfersPermitted
	case *instruments.ShareCommon:
		return pl.TransfersPermitted
	case *instruments.Coupon:
		return pl.TransfersPermitted
	case *instruments.LoyaltyPoints:
		return pl.TransfersPermitted
	case *instruments.TicketAdmission:
		return pl.TransfersPermitted
	case *instruments.CasinoChip:
		return pl.TransfersPermitted
	case *instruments.BondFixedRate:
		return pl.TransfersPermitted
	}

	return true
}

func (i *Instrument) IsFrozen(now uint64) bool {
	return i.FrozenUntil != nil && (*i.FrozenUntil == 0 || *i.FrozenUntil >= now)
}

func (i *Instrument) IsExpired(now uint64) bool {
	payload, err := i.GetPayload()
	if err != nil {
		return false
	}

	switch pl := payload.(type) {
	case *instruments.Membership:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.ShareCommon:
	case *instruments.CasinoChip:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.Coupon:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.LoyaltyPoints:
		return pl.ExpirationTimestamp != 0 && pl.ExpirationTimestamp < now
	case *instruments.TicketAdmission:
		return pl.EventEndTimestamp != 0 && pl.EventEndTimestamp < now
	}

	return false
}

func InstrumentPath(contractHash ContractHash, instrumentCode InstrumentCode) string {
	return fmt.Sprintf("%s/%s/%s", instrumentPath, contractHash, instrumentCode)
}

func (i *Instrument) MarkModified() {
	i.isModified = true
}

func (i *Instrument) ClearModified() {
	i.isModified = false
}

func (i *Instrument) IsModified() bool {
	return i.isModified
}

func (i *Instrument) CacheCopy() cacher.CacheValue {
	result := &Instrument{}

	copy(result.InstrumentType[:], i.InstrumentType[:])
	copy(result.InstrumentCode[:], i.InstrumentCode[:])

	if i.Creation != nil {
		result.Creation = i.Creation.Copy()
	}

	if i.CreationTxID != nil {
		result.CreationTxID = &bitcoin.Hash32{}
		copy(result.CreationTxID[:], i.CreationTxID[:])
	}

	if i.FrozenUntil != nil {
		frozenUntil := *i.FrozenUntil
		result.FrozenUntil = &frozenUntil
	}

	return result
}

func (i *Instrument) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(i)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, contractVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (i *Instrument) Deserialize(r io.Reader) error {
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

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "read")
	}

	i.Lock()
	defer i.Unlock()
	if _, err := bsor.UnmarshalBinary(b, i); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
