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
	FreezeCode              = BalanceAdjustmentCode('F')
	DebitCode               = BalanceAdjustmentCode('D')
	CreditCode              = BalanceAdjustmentCode('C')
	MultiContractDebitCode  = BalanceAdjustmentCode('-')
	MultiContractCreditCode = BalanceAdjustmentCode('+')

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

	Adjustments []*BalanceAdjustment `bsor:"5" json:"adjustments,omitempty"`

	pendingQuantity  uint64
	pendingDirection bool // true=credit, false=debit

	isModified bool
	sync.Mutex `bsor:"-"`
}

type Balances []*Balance

type BalanceAdjustmentCode byte

type BalanceAdjustment struct {
	Code BalanceAdjustmentCode `bsor:"1" json:"code,omitempty"`

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
		return nil, errors.New("Type must support interface")
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
	instrumentCode InstrumentCode, balances Balances) (Balances, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)

	values := make([]cacher.CacheSetValue, len(balances))
	for i, balance := range balances {
		values[i] = balance
	}

	addedValues, err := c.cacher.AddMultiSetValue(ctx, c.typ, pathPrefix, values)
	if err != nil {
		return nil, errors.Wrap(err, "add set multi")
	}

	result := make(Balances, len(addedValues))
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

	if value == nil {
		return nil, nil
	}

	return value.(*Balance), nil
}

func (c *BalanceCache) GetMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScripts []bitcoin.Script) (Balances, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		hashes[i] = LockingScriptHash(lockingScript)
	}

	values, err := c.cacher.GetMultiSetValue(ctx, c.typ, pathPrefix, hashes)
	if err != nil {
		return nil, errors.Wrap(err, "get set multi")
	}

	result := make(Balances, len(values))
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
	instrumentCode InstrumentCode, balances Balances) error {

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

	if err := c.cacher.ReleaseMultiSetValue(ctx, c.typ, pathPrefix, hashes,
		isModified); err != nil {
		return errors.Wrap(err, "release set multi")
	}

	return nil
}

func (b *Balance) PendingQuantity() uint64 {
	if b.pendingDirection { // credit
		return b.Quantity + b.pendingQuantity
	} else { // debit
		return b.Quantity - b.pendingQuantity
	}
}

// SettlePendingQuantity is the quantity to put in a settlement that is currently being built. It
// doesn't subtract frozen amounts, but does include all pending adjustements.
func (b *Balance) SettlePendingQuantity() uint64 {
	quantity := b.Quantity
	for _, adj := range b.Adjustments {
		switch adj.Code {
		case FreezeCode:
		case DebitCode, MultiContractDebitCode:
			if quantity > adj.Quantity {
				quantity -= adj.Quantity
			} else {
				quantity = 0
			}
		case CreditCode, MultiContractCreditCode:
			quantity += adj.Quantity
		}
	}

	if b.pendingDirection { // credit
		quantity += b.pendingQuantity
	} else { // debit
		quantity -= b.pendingQuantity
	}

	return quantity
}

func (b *Balance) Available() uint64 {
	available := b.PendingQuantity()
	for _, adj := range b.Adjustments {
		switch adj.Code {
		case FreezeCode, DebitCode, MultiContractDebitCode:
			if available > adj.Quantity {
				available -= adj.Quantity
			} else {
				available = 0
			}
		case CreditCode, MultiContractCreditCode:
			available += adj.Quantity
		}
	}

	return available
}

func (b *Balance) AddPendingDebit(quantity uint64) error {
	available := b.Available()
	if available < quantity {
		return fmt.Errorf("available %d, debit %d", available, quantity)
	}

	if b.pendingDirection { // credit
		if b.pendingQuantity >= quantity {
			b.pendingQuantity -= quantity
		} else {
			b.pendingDirection = false
			b.pendingQuantity = quantity - b.pendingQuantity
		}
	} else { // debit
		b.pendingQuantity += quantity
	}

	return nil
}

func (b *Balance) AddPendingCredit(quantity uint64) error {
	if b.pendingDirection { // credit
		b.pendingQuantity += quantity
	} else { // debit
		if b.pendingQuantity >= quantity {
			b.pendingQuantity -= quantity
		} else {
			b.pendingDirection = true
			b.pendingQuantity = quantity - b.pendingQuantity
		}
	}

	return nil
}

func (b *Balance) RevertPending(txid *bitcoin.Hash32) {
	var newAdjustments []*BalanceAdjustment
	for _, adj := range b.Adjustments {
		if txid.Equal(adj.TxID) {
			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments

	b.pendingDirection = false
	b.pendingQuantity = 0
}

func (b *Balance) FinalizePending(txid *bitcoin.Hash32, isMultiContract bool) {
	var code BalanceAdjustmentCode
	if isMultiContract {
		if b.pendingDirection { // credit
			code = MultiContractCreditCode
		} else { // debit
			code = MultiContractDebitCode
		}
	} else {
		if b.pendingDirection { // credit
			code = CreditCode
		} else { // debit
			code = DebitCode
		}
	}

	b.Adjustments = append(b.Adjustments, &BalanceAdjustment{
		Code:     code,
		Quantity: b.pendingQuantity,
		TxID:     txid,
	})

	b.pendingDirection = false
	b.pendingQuantity = 0
	b.isModified = true
}

func (b *Balance) Settle(transferTxID, settlementTxID bitcoin.Hash32, now uint64) {
	var newAdjustments []*BalanceAdjustment
	for _, adj := range b.Adjustments {
		if transferTxID.Equal(adj.TxID) {
			switch adj.Code {
			case DebitCode, MultiContractDebitCode:
				b.Quantity -= adj.Quantity
			case CreditCode, MultiContractCreditCode:
				b.Quantity += adj.Quantity
			}
			b.TxID = &settlementTxID
			b.Timestamp = now
			b.isModified = true

			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments
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
	return fmt.Sprintf("%s/%s/%s", balancePath, CalculateContractHash(contractLockingScript),
		instrumentCode)
}

func (v BalanceAdjustmentCode) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown BalanceAdjustmentCode value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *BalanceAdjustmentCode) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *BalanceAdjustmentCode) SetString(s string) error {
	switch s {
	case "freeze":
		*v = FreezeCode
	case "debit":
		*v = DebitCode
	case "credit":
		*v = CreditCode
	case "multi_contract_debit":
		*v = MultiContractDebitCode
	case "multi_contract_credit":
		*v = MultiContractCreditCode
	default:
		return fmt.Errorf("Unknown BalanceAdjustmentCode value \"%s\"", s)
	}

	return nil
}

func (v BalanceAdjustmentCode) String() string {
	switch v {
	case FreezeCode:
		return "freeze"
	case DebitCode:
		return "debit"
	case CreditCode:
		return "credit"
	case MultiContractDebitCode:
		return "multi_contract_debit"
	case MultiContractCreditCode:
		return "multi_contract_credit"
	default:
		return ""
	}
}

// AppendBalances is an optimization on the builtin append function which I believe appends one item
// at a time. This creates a new slice and copies the two slices into the new slice.
func AppendBalances(left, right Balances) Balances {
	llen := len(left)
	if llen == 0 {
		return right
	}

	rlen := len(right)
	if rlen == 0 {
		return left
	}

	result := make(Balances, llen+rlen, llen+rlen)
	copy(result, left)
	copy(result[llen:], right)
	return result
}

// AppendZeroBalance adds a new zero balance to the set if there isn't already a balance with the
// specified locking script.
// TODO We might want to sort these to prevent deadlocks between transfers where one transfer could
// lock a balance and be dependent on another balance locked by another transfer, but the other
// transfer is dependent on this balance. --ce
func AppendZeroBalance(balances Balances, lockingScript bitcoin.Script) Balances {
	for _, balance := range balances {
		if balance.LockingScript.Equal(lockingScript) {
			return balances // already contains so no append
		}
	}

	return append(balances, &Balance{
		LockingScript: lockingScript,
	})
}

// Find returns the balance with the specified locking script, or nil if there isn't a match.
func (bs *Balances) Find(lockingScript bitcoin.Script) *Balance {
	for _, b := range *bs {
		if b.LockingScript.Equal(lockingScript) {
			return b
		}
	}

	return nil
}

func (bs *Balances) RevertPending(txid *bitcoin.Hash32) {
	for _, b := range *bs {
		b.RevertPending(txid)
	}
}

func (bs *Balances) FinalizePending(txid *bitcoin.Hash32, isMultiContract bool) {
	for _, b := range *bs {
		b.FinalizePending(txid, isMultiContract)
	}
}

func (bs *Balances) Settle(transferTxID, settlementTxID bitcoin.Hash32, now uint64) {
	for _, b := range *bs {
		b.Settle(transferTxID, settlementTxID, now)
	}
}

func (bs *Balances) Lock() {
	for _, b := range *bs {
		b.Lock()
	}
}

func (bs *Balances) Unlock() {
	for _, b := range *bs {
		b.Unlock()
	}
}
