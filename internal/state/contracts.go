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
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	contractVersion = uint8(0)
	contractPath    = "contract"
)

type ContractCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Contract struct {
	LockingScript                bitcoin.Script                    `bsor:"2" json:"locking_script"`
	Formation                    *actions.ContractFormation        `bsor:"3" json:"formation"`
	FormationTxID                *bitcoin.Hash32                   `bsor:"4" json:"formation_txid"`
	BodyOfAgreementFormation     *actions.BodyOfAgreementFormation `bsor:"5" json:"body_of_agreement_formation"`
	BodyOfAgreementFormationTxID *bitcoin.Hash32                   `bsor:"6" json:"body_of_agreement_formation_txid"`

	InstrumentCount uint64 `bsor:"7" json:"instrument_count"`

	MovedTxID *bitcoin.Hash32 `bsor:"8" json:"moved_txid"`

	FrozenUntil *uint64         `bsor:"9" json:"frozen_until,omitempty"`
	FreezeTxID  *bitcoin.Hash32 `bsor:"10" json:"freeze_txid"`

	// TODO Populate AdminMemberInstrumentCode value. --ce
	AdminMemberInstrumentCode InstrumentCode `bsor:"11" json:"admin_member_instrument_code"`

	isModified bool
	sync.Mutex `bsor:"-"`
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

func (c *ContractCache) CopyContractData(ctx context.Context,
	fromScript, toScript bitcoin.Script) error {
	start := time.Now()

	fromHash := CalculateContractHash(fromScript)
	fromPathPrefix := fromHash.String()
	toHash := CalculateContractHash(toScript)
	toPathPrefix := toHash.String()

	if err := c.cacher.CopyRecursive(ctx, fromPathPrefix, toPathPrefix); err != nil {
		return errors.Wrap(err, "copy recursive")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		logger.Stringer("from_locking_script", fromScript),
		logger.Stringer("to_locking_script", toScript),
	}, "Copied contract data")
	return nil
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
	return fmt.Sprintf("%s/%s", CalculateContractHash(lockingScript), contractPath)
}

func (c *Contract) CheckIsAvailable(now uint64) error {
	if c.Formation == nil {
		return platform.NewRejectError(actions.RejectionsContractDoesNotExist, "")
	}

	if c.IsExpired(now) {
		return platform.NewRejectError(actions.RejectionsContractExpired, "")
	}

	if c.MovedTxID != nil {
		return platform.NewRejectError(actions.RejectionsContractMoved, c.MovedTxID.String())
	}

	if c.IsFrozen(now) {
		return platform.NewRejectError(actions.RejectionsContractFrozen, "")
	}

	return nil
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

func (c *Contract) Freeze(txid bitcoin.Hash32, until uint64) {
	c.FreezeTxID = &txid
	c.FrozenUntil = &until
}

func (c *Contract) IsFrozen(now uint64) bool {
	return c.FrozenUntil != nil && (*c.FrozenUntil == 0 || *c.FrozenUntil >= now)
}

func (c *Contract) Thaw() {
	c.FreezeTxID = nil
	c.FrozenUntil = nil
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

	result.LockingScript = make(bitcoin.Script, len(c.LockingScript))
	copy(result.LockingScript, c.LockingScript)

	if c.Formation != nil {
		result.Formation = c.Formation.Copy()
	}

	if c.FormationTxID != nil {
		result.FormationTxID = &bitcoin.Hash32{}
		copy(result.FormationTxID[:], c.FormationTxID[:])
	}

	if c.BodyOfAgreementFormation != nil {
		result.BodyOfAgreementFormation = c.BodyOfAgreementFormation.Copy()
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

		// A single balance lock doesn't need to use the balance locker since it isn't susceptible
		// to the group deadlock.
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
