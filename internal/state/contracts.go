package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	contractVersion = uint8(0)
	contractPath    = "contracts"
)

type ContractCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Contract struct {
	// KeyHash is the hash that is added to the root key to derive the contract's key.
	KeyHash                      bitcoin.Hash32                    `bsor:"1" json:"key_hash"`
	LockingScript                bitcoin.Script                    `bsor:"2" json:"locking_script"`
	Formation                    *actions.ContractFormation        `bsor:"-" json:"formation"`
	FormationTxID                *bitcoin.Hash32                   `bsor:"4" json:"formation_txid"`
	BodyOfAgreementFormation     *actions.BodyOfAgreementFormation `bsor:"-" json:"body_of_agreement_formation"`
	BodyOfAgreementFormationTxID *bitcoin.Hash32                   `bsor:"6" json:"body_of_agreement_formation_txid"`

	Instruments []*Instrument `bsor:"7" json:"instruments"`

	// FormationScript is only used by Serialize to save the Formation value in BSOR.
	FormationScript bitcoin.Script `bsor:"8" json:"formation_script"`

	// BodyOfAgreementFormationScript is only used by Serialize to save the BodyOfAgreementFormation
	// value in BSOR.
	BodyOfAgreementFormationScript bitcoin.Script `bsor:"9" json:"body_of_agreement_formation_script"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

type ContractLookup struct {
	KeyHash       bitcoin.Hash32 `json:"key_hash"`
	LockingScript bitcoin.Script `json:"locking_script"`
}

func NewContractCache(cache *cacher.Cache) (*ContractCache, error) {
	typ := reflect.TypeOf(&Contract{})

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

	return &ContractCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *ContractCache) List(ctx context.Context,
	store storage.StreamStorage) ([]*ContractLookup, error) {

	paths, err := store.List(ctx, contractPath)
	if err != nil {
		return nil, errors.Wrap(err, "list")
	}

	var result []*ContractLookup
	for _, path := range paths {
		item, err := c.cacher.Get(ctx, c.typ, path)
		if err != nil {
			return nil, errors.Wrap(err, "get")
		}

		if item == nil {
			return nil, fmt.Errorf("Not found: %s", path)
		}

		contract := item.(*Contract)
		contract.Lock()
		result = append(result, &ContractLookup{
			KeyHash:       contract.KeyHash,
			LockingScript: contract.LockingScript,
		})
		contract.Unlock()

		c.cacher.Release(ctx, path)
	}

	return result, nil
}

func (c *ContractCache) Save(ctx context.Context, contract *Contract) {
	c.cacher.Save(ctx, contract)
}

func (c *ContractCache) Add(ctx context.Context, contract *Contract) (*Contract, error) {
	item, err := c.cacher.Add(ctx, c.typ, contract)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Contract), nil
}

func (c *ContractCache) Get(ctx context.Context, lockingScript bitcoin.Script) (*Contract, error) {
	item, err := c.cacher.Get(ctx, c.typ, ContractPath(lockingScript))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Contract), nil
}

func (c *ContractCache) Release(ctx context.Context, lockingScript bitcoin.Script) {
	c.cacher.Release(ctx, ContractPath(lockingScript))
}

func ContractPath(lockingScript bitcoin.Script) string {
	return fmt.Sprintf("%s/%s", contractPath, CalculateContractHash(lockingScript))
}

func (c *Contract) AdminLockingScript() bitcoin.Script {
	if c.Formation == nil {
		return nil
	}

	ra, err := bitcoin.DecodeRawAddress(c.Formation.AdminAddress)
	if err != nil {
		return nil
	}

	lockingScript, err := ra.LockingScript()
	if err != nil {
		return nil
	}

	return lockingScript
}

func (c *Contract) IsExpired(now uint64) bool {
	return c.Formation != nil && c.Formation.ContractExpiration != 0 &&
		c.Formation.ContractExpiration < now
}

func (c *Contract) GetInstrument(instrumentCode InstrumentCode) *Instrument {
	for _, instrument := range c.Instruments {
		if bytes.Equal(instrument.InstrumentCode[:], instrumentCode[:]) {
			return instrument
		}
	}

	return nil
}

func (c *Contract) Path() string {
	return ContractPath(c.LockingScript)
}

func (c *Contract) MarkModified() {
	c.isModified = true
}

func (c *Contract) ClearModified() {
	c.isModified = false
}

func (c *Contract) IsModified() bool {
	return c.isModified
}

func (c *Contract) Serialize(w io.Writer) error {
	if c.Formation != nil {
		script, err := protocol.Serialize(c.Formation, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize contract formation")
		}

		c.FormationScript = script
	}

	if c.BodyOfAgreementFormation != nil {
		script, err := protocol.Serialize(c.BodyOfAgreementFormation, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize body of agreement formation")
		}

		c.BodyOfAgreementFormationScript = script
	}

	for i, instrument := range c.Instruments {
		if err := instrument.PrepareForMarshal(); err != nil {
			return errors.Wrapf(err, "prepare instrument %d", i)
		}
	}

	b, err := bsor.MarshalBinary(c)
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

func (c *Contract) Deserialize(r io.Reader) error {
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

	c.Lock()
	defer c.Unlock()
	if _, err := bsor.UnmarshalBinary(b, c); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	if len(c.FormationScript) != 0 {
		action, err := protocol.Deserialize(c.FormationScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize contract formation")
		}

		formation, ok := action.(*actions.ContractFormation)
		if !ok {
			return errors.New("FormationScript is wrong type")
		}

		c.Formation = formation
	}

	if len(c.BodyOfAgreementFormationScript) != 0 {
		action, err := protocol.Deserialize(c.BodyOfAgreementFormationScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize body of agreement formation")
		}

		bodyOfAgreementFormation, ok := action.(*actions.BodyOfAgreementFormation)
		if !ok {
			return errors.New("BodyOfAgreementFormationScript is wrong type")
		}

		c.BodyOfAgreementFormation = bodyOfAgreementFormation
	}

	for i, instrument := range c.Instruments {
		if err := instrument.CompleteUnmarshal(); err != nil {
			return errors.Wrapf(err, "complete instrument %d", i)
		}
	}

	return nil
}

func (id ContractHash) String() string {
	return bitcoin.Hash32(id).String()
}

func (id ContractHash) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

func (id *ContractHash) UnmarshalText(text []byte) error {
	h, err := bitcoin.NewHash32FromStr(string(text))
	if err != nil {
		return err
	}

	*id = ContractHash(*h)
	return nil
}
