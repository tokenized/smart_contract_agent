package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
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

	InstrumentCount uint64 `bsor:"7" json:"instrument_count"`

	MovedTxID *bitcoin.Hash32 `bsor:"8" json:"moved_txid"`

	FrozenUntil *uint64 `bsor:"9" json:"frozen_until,omitempty"`

	// TODO Populate AdminMemberInstrumentCode value. --ce
	AdminMemberInstrumentCode InstrumentCode `bsor:"10" json:"admin_member_instrument_code"`

	// FormationScript is only used by Serialize to save the Formation value in BSOR.
	FormationScript bitcoin.Script `bsor:"11" json:"formation_script"`

	// BodyOfAgreementFormationScript is only used by Serialize to save the BodyOfAgreementFormation
	// value in BSOR.
	BodyOfAgreementFormationScript bitcoin.Script `bsor:"12" json:"body_of_agreement_formation_script"`

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

func CopyContractData(ctx context.Context, store storage.CopyList,
	fromScript, toScript bitcoin.Script) error {

	// TODO Check if any of these objects are in the cache and modified. --ce

	fromHash := CalculateContractHash(fromScript)
	toHash := CalculateContractHash(toScript)

	start := time.Now()

	from := fmt.Sprintf("%s/%s", instrumentPath, fromHash)
	to := fmt.Sprintf("%s/%s", instrumentPath, toHash)
	if err := CopyRecursive(ctx, store, from, to); err != nil {
		return errors.Wrap(err, "copy instruments")
	}

	from = fmt.Sprintf("%s/%s", balancePath, fromHash)
	to = fmt.Sprintf("%s/%s", balancePath, toHash)
	if err := CopyRecursive(ctx, store, from, to); err != nil {
		return errors.Wrap(err, "copy balances")
	}

	from = fmt.Sprintf("%s/%s", votePath, fromHash)
	to = fmt.Sprintf("%s/%s", votePath, toHash)
	if err := CopyRecursive(ctx, store, from, to); err != nil {
		return errors.Wrap(err, "copy votes")
	}

	from = fmt.Sprintf("%s/%s", subscriptionPath, fromHash)
	to = fmt.Sprintf("%s/%s", subscriptionPath, toHash)
	if err := CopyRecursive(ctx, store, from, to); err != nil {
		return errors.Wrap(err, "copy subscriptions")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		logger.Stringer("from_locking_script", fromScript),
		logger.Stringer("to_locking_script", toScript),
	}, "Copied contract data")
	return nil
}

func (c *ContractCache) List(ctx context.Context, store storage.List) ([]*ContractLookup, error) {
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
	contract.Lock()
	lockingScript := contract.LockingScript
	contract.Unlock()

	c.cacher.Save(ctx, ContractPath(lockingScript), contract)
}

func (c *ContractCache) Add(ctx context.Context, contract *Contract) (*Contract, error) {
	contract.Lock()
	lockingScript := contract.LockingScript
	contract.Unlock()

	item, err := c.cacher.Add(ctx, c.typ, ContractPath(lockingScript), contract)
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

func (c *Contract) IsAdmin(lockingScript bitcoin.Script) bool {
	if c.Formation == nil {
		return false
	}

	ra, err := bitcoin.RawAddressFromLockingScript(lockingScript)
	if err != nil {
		return false
	}
	b := ra.Bytes()

	return bytes.Equal(c.Formation.AdminAddress, b)
}

func (c *Contract) IsAdminOrOperator(lockingScript bitcoin.Script) bool {
	if c.Formation == nil {
		return false
	}

	ra, err := bitcoin.RawAddressFromLockingScript(lockingScript)
	if err != nil {
		return false
	}
	b := ra.Bytes()

	return bytes.Equal(c.Formation.AdminAddress, b) || bytes.Equal(c.Formation.OperatorAddress, b)
}

func (c *Contract) IsExpired(now uint64) bool {
	return c.Formation != nil && c.Formation.ContractExpiration != 0 &&
		c.Formation.ContractExpiration < now
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

func (c *Contract) CacheCopy() cacher.CacheValue {
	result := &Contract{
		InstrumentCount: c.InstrumentCount,
	}

	copy(result.KeyHash[:], c.KeyHash[:])

	result.LockingScript = make(bitcoin.Script, len(c.LockingScript))
	copy(result.LockingScript, c.LockingScript)

	isTest := IsTest()

	if c.Formation != nil {
		copyScript, _ := protocol.Serialize(c.Formation, isTest)
		action, _ := protocol.Deserialize(copyScript, isTest)
		result.Formation, _ = action.(*actions.ContractFormation)
	}

	if c.FormationTxID != nil {
		result.FormationTxID = &bitcoin.Hash32{}
		copy(result.FormationTxID[:], c.FormationTxID[:])
	}

	if c.BodyOfAgreementFormation != nil {
		copyScript, _ := protocol.Serialize(c.BodyOfAgreementFormation, isTest)
		action, _ := protocol.Deserialize(copyScript, isTest)
		result.BodyOfAgreementFormation, _ = action.(*actions.BodyOfAgreementFormation)
	}

	if c.BodyOfAgreementFormationTxID != nil {
		result.BodyOfAgreementFormationTxID = &bitcoin.Hash32{}
		copy(result.BodyOfAgreementFormationTxID[:], c.BodyOfAgreementFormationTxID[:])
	}

	if c.MovedTxID != nil {
		result.MovedTxID = &bitcoin.Hash32{}
		copy(result.MovedTxID[:], c.MovedTxID[:])
	}

	if c.FrozenUntil != nil {
		frozenUntil := *c.FrozenUntil
		result.FrozenUntil = &frozenUntil
	}

	return result
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

	return nil
}

// HasAnyContractBalance returns true if the specified locking script has any non-zero settled
// balance with the specified contract.
func HasAnyContractBalance(ctx context.Context, caches *Caches, contract *Contract,
	lockingScript bitcoin.Script) (bool, error) {

	contract.Lock()
	contractLockingScript := contract.LockingScript
	instrumentCount := contract.InstrumentCount
	contract.Unlock()

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		return false, errors.Wrap(err, "contract address")
	}

	for i := uint64(0); i < instrumentCount; i++ {
		instrumentCode := InstrumentCode(protocol.InstrumentCodeFromContract(contractAddress, i))

		balance, err := caches.Balances.Get(ctx, contractLockingScript, instrumentCode,
			lockingScript)
		if err != nil {
			return false, errors.Wrap(err, "get balance")
		}

		if balance == nil {
			continue
		}

		balance.Lock()
		quantity := balance.Quantity
		balance.Unlock()
		caches.Balances.Release(ctx, contractLockingScript, instrumentCode, balance)

		if quantity > 0 {
			return true, nil
		}
	}

	return false, nil
}
