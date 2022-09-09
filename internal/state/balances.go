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
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	FreezeCode               = BalanceHoldingCode('F')
	DebitCode                = BalanceHoldingCode('D')
	DepositCode              = BalanceHoldingCode('P')
	MultiContractDebitCode   = BalanceHoldingCode('-')
	MultiContractDepositCode = BalanceHoldingCode('+')

	balanceVersion = uint8(0)
	balancePath    = "balances"
)

type BalanceCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Balance struct {
	LockingScript bitcoin.Script  `bsor:"1" json:"locking_script"`
	Quantity      uint64          `bsor:"2" json:"quantity"`
	Timestamp     uint64          `bsor:"3" json:"timestamp"`
	TxID          *bitcoin.Hash32 `bsor:"4" json:"txID,omitempty"`

	Holdings []*BalanceHolding `bsor:"5" json:"holdings,omitempty"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

type BalanceHoldingCode byte

type BalanceHolding struct {
	Code BalanceHoldingCode `bsor:"1" json:"code,omitempty"`

	Expires  protocol.Timestamp `bsor:"2" json:"expires,omitempty"`
	Quantity uint64             `bsor:"3" json:"quantity,omitempty"`
	TxID     *bitcoin.Hash32    `bsor:"4" json:"txID,omitempty"`
}

func NewBalanceCache(cache *cacher.Cache) (*BalanceCache, error) {
	typ := reflect.TypeOf(&Balance{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must be support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.CacheSetValue); !ok {
		return nil, errors.New("Type must implement CacheSetValue")
	}

	return &BalanceCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *BalanceCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balance *Balance) (*Balance, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)

	value, err := c.cacher.AddSetValue(ctx, c.typ, pathPrefix, balance)
	if err != nil {
		return nil, errors.Wrap(err, "add set")
	}

	return value.(*Balance), nil
}

func (c *BalanceCache) AddMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances []*Balance) ([]*Balance, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)

	values := make([]cacher.CacheSetValue, len(balances))
	for i, balance := range balances {
		values[i] = balance
	}

	addedValues, err := c.cacher.AddSetMultiValue(ctx, c.typ, pathPrefix, values)
	if err != nil {
		return nil, errors.Wrap(err, "add set multi")
	}

	result := make([]*Balance, len(addedValues))
	for i, value := range addedValues {
		result[i] = value.(*Balance)
	}

	return result, nil
}

func (c *BalanceCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScript bitcoin.Script) (*Balance, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	hash := LockingScriptHash(lockingScript)

	value, err := c.cacher.GetSetValue(ctx, c.typ, pathPrefix, hash)
	if err != nil {
		return nil, errors.Wrap(err, "get set")
	}

	return value.(*Balance), nil
}

func (c *BalanceCache) GetMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScripts []bitcoin.Script) ([]*Balance, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		hashes[i] = LockingScriptHash(lockingScript)
	}

	values, err := c.cacher.GetSetMultiValue(ctx, c.typ, pathPrefix, hashes)
	if err != nil {
		return nil, errors.Wrap(err, "get set multi")
	}

	result := make([]*Balance, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}

		result[i] = value.(*Balance)
	}

	return result, nil
}

func (c *BalanceCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balance *Balance) error {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	balance.Lock()
	hash := LockingScriptHash(balance.LockingScript)
	isModified := balance.isModified
	balance.Unlock()

	if err := c.cacher.ReleaseSetValue(ctx, c.typ, pathPrefix, hash, isModified); err != nil {
		return errors.Wrap(err, "release set")
	}

	return nil
}

func (c *BalanceCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances []*Balance) error {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	hashes := make([]bitcoin.Hash32, len(balances))
	isModified := make([]bool, len(balances))
	for i, balance := range balances {
		if balance == nil {
			continue
		}

		balance.Lock()
		hashes[i] = LockingScriptHash(balance.LockingScript)
		isModified[i] = balance.isModified
		balance.Unlock()
	}

	if err := c.cacher.ReleaseSetMultiValue(ctx, c.typ, pathPrefix, hashes, isModified); err != nil {
		return errors.Wrap(err, "release set multi")
	}

	return nil
}

func (b *Balance) MarkModified() {
	b.isModified = true
}

func (b *Balance) IsModified() bool {
	return b.isModified
}

func (b *Balance) ClearModified() {
	b.isModified = false
}

func (b *Balance) Hash() bitcoin.Hash32 {
	return LockingScriptHash(b.LockingScript)
}

func (b *Balance) Serialize(w io.Writer) error {
	bs, err := bsor.MarshalBinary(b)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, balanceVersion); err != nil {
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

func (b *Balance) Deserialize(r io.Reader) error {
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

func balancePathPrefix(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode) string {
	return fmt.Sprintf("%s/%s/%s", balancePath, CalculateContractID(contractLockingScript),
		instrumentCode)
}

func (v BalanceHoldingCode) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown BalanceHoldingCode value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *BalanceHoldingCode) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *BalanceHoldingCode) SetString(s string) error {
	switch s {
	case "freeze":
		*v = FreezeCode
	case "debit":
		*v = DebitCode
	case "deposit":
		*v = DepositCode
	case "multi_contract_debit":
		*v = MultiContractDebitCode
	case "multi_contract_deposit":
		*v = MultiContractDepositCode
	default:
		return fmt.Errorf("Unknown BalanceHoldingCode value \"%s\"", s)
	}

	return nil
}

func (v BalanceHoldingCode) String() string {
	switch v {
	case FreezeCode:
		return "freeze"
	case DebitCode:
		return "debit"
	case DepositCode:
		return "deposit"
	case MultiContractDebitCode:
		return "multi_contract_debit"
	case MultiContractDepositCode:
		return "multi_contract_deposit"
	default:
		return ""
	}
}
