package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

const (
	FreezeCode              = BalanceAdjustmentCode('F')
	DebitCode               = BalanceAdjustmentCode('D')
	CreditCode              = BalanceAdjustmentCode('C')
	MultiContractDebitCode  = BalanceAdjustmentCode('-')
	MultiContractCreditCode = BalanceAdjustmentCode('+')
	ConfiscationCode        = BalanceAdjustmentCode('S') // Seize

	balanceVersion = uint8(0)
	balancePath    = "balances"
)

type BalanceCache struct {
	cacher cacher.Cacher
	typ    reflect.Type
}

type Balance struct {
	LockingScript bitcoin.Script  `bsor:"1" json:"locking_script"`
	Quantity      uint64          `bsor:"2" json:"quantity"`
	Timestamp     uint64          `bsor:"3" json:"timestamp"`
	TxID          *bitcoin.Hash32 `bsor:"4" json:"txid,omitempty"`

	Adjustments []*BalanceAdjustment `bsor:"5" json:"adjustments,omitempty"`

	// ActionIndex is the index to output containing the action payload.
	ActionIndex uint32 `bsor:"6" json:"action_index"`

	pendingQuantity  uint64
	pendingDirection bool // true=credit, false=debit

	markModified atomic.Value
	isModified   atomic.Value
	sync.Mutex   `bsor:"-"`
}

type Balances []*Balance

type BalanceSet []Balances

type BalanceAdjustmentCode byte

type BalanceAdjustment struct {
	Code BalanceAdjustmentCode `bsor:"1" json:"code,omitempty"`

	Expires         *uint64         `bsor:"2" json:"expires,omitempty"`
	Quantity        uint64          `bsor:"3" json:"quantity,omitempty"`
	TxID            *bitcoin.Hash32 `bsor:"4" json:"txID,omitempty"`
	SettledQuantity uint64          `bsor:"5" json:"settled_quantity,omitempty"`

	// ActionIndex is the index to output containing the action payload.
	ActionIndex uint32 `bsor:"6" json:"action_index"`
}

func NewBalanceCache(cache cacher.Cacher) (*BalanceCache, error) {
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
	if _, ok := itemInterface.(cacher.SetValue); !ok {
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

	values := make([]cacher.SetValue, len(balances))
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

func (c *BalanceCache) List(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode) (Balances, error) {

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	values, err := c.cacher.ListMultiSetValue(ctx, c.typ, pathPrefix)
	if err != nil {
		return nil, errors.Wrap(err, "list set multi")
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
	balance.Unlock()

	if err := c.cacher.ReleaseSetValue(ctx, c.typ, pathPrefix, hash); err != nil {
		return errors.Wrap(err, "release set")
	}

	return nil
}

func (c *BalanceCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances Balances) error {

	if len(balances) == 0 {
		return nil
	}

	pathPrefix := balancePathPrefix(contractLockingScript, instrumentCode)
	var hashes []bitcoin.Hash32
	for _, balance := range balances {
		if balance == nil {
			continue
		}

		balance.Lock()
		hashes = append(hashes, LockingScriptHash(balance.LockingScript))
		balance.Unlock()
	}

	if err := c.cacher.ReleaseMultiSetValue(ctx, c.typ, pathPrefix, hashes); err != nil {
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
		case FreezeCode, DebitCode, MultiContractDebitCode:
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

// Available returns the balance quantity with all pending modifications and balance adjustments
// applied.
func (b *Balance) Available(now uint64) uint64 {
	available := b.PendingQuantity()
	for _, adj := range b.Adjustments {
		switch adj.Code {
		case FreezeCode:
			if adj.Expires != nil && (*adj.Expires == 0 || *adj.Expires >= now) {
				if available > adj.Quantity {
					available -= adj.Quantity
				} else {
					available = 0
				}
			}
		case DebitCode, MultiContractDebitCode:
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

func (b *Balance) FrozenQuantity(now uint64) uint64 {
	var result uint64
	for _, adj := range b.Adjustments {
		switch adj.Code {
		case FreezeCode:
			if adj.Expires != nil && (*adj.Expires == 0 || *adj.Expires >= now) {
				result += adj.Quantity
			}
		}
	}

	return result
}

func (b *Balance) PendingTransferAction() *wire.OutPoint {
	for _, adj := range b.Adjustments {
		if adj.TxID == nil {
			continue
		}

		switch adj.Code {
		case DebitCode, MultiContractDebitCode, CreditCode, MultiContractCreditCode:
			return &wire.OutPoint{
				Hash:  *adj.TxID,
				Index: adj.ActionIndex,
			}
		}
	}

	return nil
}

// AddPendingDebit adds a pending modification to reduce the balance.
func (b *Balance) AddPendingDebit(quantity, now uint64) error {
	if pendingTransferAction := b.PendingTransferAction(); pendingTransferAction != nil {
		return platform.NewDependsOnTxError(pendingTransferAction.Hash, pendingTransferAction.Index)
	}

	available := b.Available(now)
	if available < quantity {
		if b.FrozenQuantity(now) > 0 {
			return platform.NewRejectError(actions.RejectionsHoldingsFrozen,
				fmt.Sprintf("available %d, debit %d", available, quantity))
		} else {
			return platform.NewRejectError(actions.RejectionsInsufficientQuantity,
				fmt.Sprintf("available %d, debit %d", available, quantity))
		}
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

// AddPendingCredit adds a pending modification to increase the balance.
func (b *Balance) AddPendingCredit(quantity uint64, now uint64) error {
	if pendingTransferAction := b.PendingTransferAction(); pendingTransferAction != nil {
		return platform.NewDependsOnTxError(pendingTransferAction.Hash, pendingTransferAction.Index)
	}

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

// RevertPending removes any balance adjustements for the specified transfer transaction and
// clears any pending modification.
func (b *Balance) RevertPending() {
	b.pendingDirection = false
	b.pendingQuantity = 0
}

func (b *Balance) RevertPendingAdjustment(txid bitcoin.Hash32, index int) {
	found := false
	var newAdjustments []*BalanceAdjustment
	for _, adj := range b.Adjustments {
		if txid.Equal(adj.TxID) && int(adj.ActionIndex) == index {
			found = true
			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}

	if !found {
		return
	}

	b.MarkModified()
	b.Adjustments = newAdjustments
}

func (b *Balance) Revert(txid bitcoin.Hash32, index int) {
	b.RevertPending()
	b.RevertPendingAdjustment(txid, index)
}

func (b *Balance) AddFreeze(txid bitcoin.Hash32, index int, quantity, frozenUntil uint64) uint64 {
	for _, adj := range b.Adjustments {
		if !txid.Equal(adj.TxID) {
			continue
		}

		// Already have this freeze
		return 0
	}

	// Add a new freeze
	settledQuantity := b.SettlePendingQuantity()
	if quantity > settledQuantity {
		settledQuantity = 0
	} else {
		settledQuantity -= quantity
	}
	b.Adjustments = append(b.Adjustments, &BalanceAdjustment{
		Code:            FreezeCode,
		Expires:         &frozenUntil,
		Quantity:        quantity,
		TxID:            &txid,
		SettledQuantity: settledQuantity,
		ActionIndex:     uint32(index),
	})

	b.MarkModified()
	return settledQuantity
}

func (b *Balance) SettleFreeze(orderTxID bitcoin.Hash32, orderIndex int, freezeTxID bitcoin.Hash32,
	freezeIndex int) {

	for _, adj := range b.Adjustments {
		if !orderTxID.Equal(adj.TxID) && orderIndex == int(adj.ActionIndex) {
			continue
		}

		adj.TxID = &freezeTxID
		adj.ActionIndex = uint32(freezeIndex)
		return
	}
}

func (b *Balance) RemoveFreeze(txid bitcoin.Hash32, actionIndex int) {
	var newAdjustments []*BalanceAdjustment
	found := false
	for _, adj := range b.Adjustments {
		if adj.Code == FreezeCode && txid.Equal(adj.TxID) && int(adj.ActionIndex) == actionIndex {
			found = true
			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}

	if found {
		b.Adjustments = newAdjustments
		b.MarkModified()
	}
}

func (b *Balance) AddConfiscation(txid bitcoin.Hash32, index int, quantity uint64) (uint64, error) {
	for _, adj := range b.Adjustments {
		if !txid.Equal(adj.TxID) && int(adj.ActionIndex) == index {
			continue
		}

		// Already have this confiscation
		return adj.SettledQuantity, nil
	}

	// Add a new confiscation
	settledQuantity := b.SettlePendingQuantity()
	if settledQuantity < quantity {
		return 0, platform.NewRejectError(actions.RejectionsInsufficientQuantity,
			fmt.Sprintf("available %d, confiscation %d", settledQuantity, quantity))
	}
	settledQuantity -= quantity

	b.Adjustments = append(b.Adjustments, &BalanceAdjustment{
		Code:            ConfiscationCode,
		Quantity:        quantity,
		TxID:            &txid,
		SettledQuantity: settledQuantity,
		ActionIndex:     uint32(index),
	})

	b.MarkModified()
	return settledQuantity, nil
}

func (b *Balance) FinalizeConfiscation(orderTxID, confiscationTxID bitcoin.Hash32,
	confiscationIndex int, now uint64) bool {

	var newAdjustments []*BalanceAdjustment
	found := false
	for _, adj := range b.Adjustments {
		if orderTxID.Equal(adj.TxID) {
			b.Quantity -= adj.Quantity
			b.TxID = &confiscationTxID
			b.ActionIndex = uint32(confiscationIndex)
			b.Timestamp = now
			b.MarkModified()
			found = true

			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments

	return found
}

// SettlePending clears the current pending modification and converts it into a balance adjustment.
func (b *Balance) SettlePending(txid bitcoin.Hash32, index int, isMultiContract bool) {
	for _, adj := range b.Adjustments {
		if adj.TxID.Equal(&txid) && int(adj.ActionIndex) == index {
			return
		}
	}

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

	settledQuantity := b.SettlePendingQuantity()

	b.Adjustments = append(b.Adjustments, &BalanceAdjustment{
		Code:            code,
		Quantity:        b.pendingQuantity,
		TxID:            &txid,
		SettledQuantity: settledQuantity,
		ActionIndex:     uint32(index),
	})

	b.pendingDirection = false
	b.pendingQuantity = 0
	b.MarkModified()
}

// CancelPending removes any balance adjustements for the specified txid.
func (b *Balance) CancelPending(txid bitcoin.Hash32, index int) {
	var newAdjustments []*BalanceAdjustment
	for _, adj := range b.Adjustments {
		if txid.Equal(adj.TxID) && int(adj.ActionIndex) == index {
			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments
	b.MarkModified()
}

func (b *Balance) VerifySettlement(transferTxID bitcoin.Hash32, index int,
	quantity, now uint64) int {

	for _, adj := range b.Adjustments {
		if !transferTxID.Equal(adj.TxID) && int(adj.ActionIndex) == index {
			continue
		}

		if adj.SettledQuantity == quantity {
			return actions.RejectionsSuccess
		}

		if b.FrozenQuantity(now) > 0 {
			return actions.RejectionsTransferExpired
		}

		return actions.RejectionsMsgMalformed
	}

	// Assume the adjustment was removed by a multi-contract transfer expiration.
	return actions.RejectionsTransferExpired
}

// Settle applies any balance adjustments for the specified request transaction to the current
// quantity.
func (b *Balance) Settle(ctx context.Context, requestTxID bitcoin.Hash32, requestIndex int,
	responseTxID bitcoin.Hash32, responseIndex int, now uint64) bool {

	var newAdjustments []*BalanceAdjustment
	found := false
	for _, adj := range b.Adjustments {
		if requestTxID.Equal(adj.TxID) && int(adj.ActionIndex) == requestIndex {
			switch adj.Code {
			case DebitCode, MultiContractDebitCode:
				b.Quantity -= adj.Quantity
			case CreditCode, MultiContractCreditCode:
				b.Quantity += adj.Quantity
			}
			b.TxID = &responseTxID
			b.ActionIndex = uint32(responseIndex)
			b.Timestamp = now
			found = true

			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments

	if found {
		b.MarkModified()
		return true
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("locking_script", b.LockingScript),
		logger.Stringer("request_txid", requestTxID),
	}, "Balance adjustment not found to settle")

	return false
}

// Settle applies any balance adjustments for the specified request transaction to the current
// quantity, or if the balance adjustment isn't found then it updates the balance.
func (b *Balance) HardSettle(ctx context.Context, requestTxID bitcoin.Hash32, requestIndex int,
	responseTxID bitcoin.Hash32, responseIndex int, quantity uint64, now uint64) bool {

	var newAdjustments []*BalanceAdjustment
	found := false
	for _, adj := range b.Adjustments {
		if requestTxID.Equal(adj.TxID) && int(adj.ActionIndex) == requestIndex {
			switch adj.Code {
			case DebitCode, MultiContractDebitCode:
				b.Quantity -= adj.Quantity
			case CreditCode, MultiContractCreditCode:
				b.Quantity += adj.Quantity
			}
			b.TxID = &responseTxID
			b.ActionIndex = uint32(responseIndex)
			b.Timestamp = now
			found = true

			continue
		}

		newAdjustments = append(newAdjustments, adj)
	}
	b.Adjustments = newAdjustments

	if found {
		b.MarkModified()
		return true
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("locking_script", b.LockingScript),
		logger.Stringer("request_txid", requestTxID),
	}, "Balance adjustment not found to settle. Settling manually")

	b.Quantity = quantity
	b.Timestamp = now
	b.TxID = &responseTxID
	b.ActionIndex = uint32(responseIndex)
	b.MarkModified()
	return false
}

func (b *Balance) Initialize() {
	b.isModified.Store(false)
}

func (b *Balance) ProvideMarkModified(markModified cacher.MarkModified) {
	b.markModified.Store(markModified)
}

func (b *Balance) MarkModified() {
	if v := b.markModified.Load(); v != nil {
		v.(cacher.MarkModified)()
	}

	b.isModified.Store(true)
}

func (b *Balance) GetModified() bool {
	if v := b.isModified.Swap(false); v != nil {
		return v.(bool)
	}

	return false
}

func (b *Balance) IsModified() bool {
	if v := b.isModified.Load(); v != nil {
		return v.(bool)
	}

	return false
}

func (b *Balance) Hash() bitcoin.Hash32 {
	return LockingScriptHash(b.LockingScript)
}

func (b *Balance) CacheSetCopy() cacher.SetValue {
	result := &Balance{
		Quantity:    b.Quantity,
		Timestamp:   b.Timestamp,
		Adjustments: make([]*BalanceAdjustment, len(b.Adjustments)),
	}

	result.LockingScript = make(bitcoin.Script, len(b.LockingScript))
	copy(result.LockingScript, b.LockingScript)

	if b.TxID != nil {
		result.TxID = &bitcoin.Hash32{}
		copy(result.TxID[:], b.TxID[:])
	}

	for i, adjustment := range b.Adjustments {
		newAdjustment := &BalanceAdjustment{
			Code:            adjustment.Code,
			Expires:         adjustment.Expires,
			Quantity:        adjustment.Quantity,
			SettledQuantity: adjustment.SettledQuantity,
			ActionIndex:     adjustment.ActionIndex,
		}

		if adjustment.TxID != nil {
			newAdjustment.TxID = &bitcoin.Hash32{}
			copy(newAdjustment.TxID[:], adjustment.TxID[:])
		}

		result.Adjustments[i] = newAdjustment
	}

	return result
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
	return fmt.Sprintf("%s/%s/%s", CalculateContractHash(contractLockingScript), instrumentCode,
		balancePath)
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
func AppendZeroBalance(balances Balances, lockingScript bitcoin.Script) Balances {
	for _, balance := range balances {
		if balance.LockingScript.Equal(lockingScript) {
			return balances // already contains so no append
		}
	}

	return append(balances, ZeroBalance(lockingScript))
}

func ZeroBalance(lockingScript bitcoin.Script) *Balance {
	result := &Balance{
		LockingScript: lockingScript,
	}
	result.Initialize()
	return result
}

// Find returns the balance with the specified locking script, or nil if there isn't a match.
func (bs *Balances) Find(lockingScript bitcoin.Script) *Balance {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		if b.LockingScript.Equal(lockingScript) {
			return b
		}
	}

	return nil
}

func (bs *Balances) RevertPending() {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.RevertPending()
	}
}

func (bs *Balances) RevertPendingAdjustment(txid bitcoin.Hash32, index int) {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.RevertPendingAdjustment(txid, index)
	}
}

func (bs *Balances) Revert(txid bitcoin.Hash32, index int) {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.Revert(txid, index)
	}
}

func (bs *Balances) SettlePending(txid bitcoin.Hash32, index int, isMultiContract bool) {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.SettlePending(txid, index, isMultiContract)
	}
}

func (bs *Balances) CancelPending(txid bitcoin.Hash32, index int) {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.CancelPending(txid, index)
	}
}

func (bs *Balances) Settle(ctx context.Context, requestTxID bitcoin.Hash32, requestIndex int,
	responseTxID bitcoin.Hash32, responseIndex int, now uint64) {

	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.Settle(ctx, requestTxID, requestIndex, responseTxID, responseIndex, now)
	}
}

func (bs *Balances) SettleFreeze(orderTxID bitcoin.Hash32, orderIndex int,
	freezeOrderTxID bitcoin.Hash32, freezeIndex int) {

	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.SettleFreeze(orderTxID, orderIndex, freezeOrderTxID, freezeIndex)
	}
}

func (bs *Balances) RemoveFreeze(freezeOrderTxID bitcoin.Hash32, freezeIndex int) {
	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.RemoveFreeze(freezeOrderTxID, freezeIndex)
	}
}

func (bs *Balances) FinalizeConfiscation(orderTxID, confiscationTxID bitcoin.Hash32,
	confiscationIndex int, now uint64) {

	for _, b := range *bs {
		if b == nil {
			continue
		}

		b.FinalizeConfiscation(orderTxID, confiscationTxID, confiscationIndex, now)
	}
}

func (bs *Balances) LockingScripts() []bitcoin.Script {
	var result []bitcoin.Script
	for _, b := range *bs {
		if b == nil {
			continue
		}

		result = appendLockingScript(result, b.LockingScript)
	}
	return result
}

func (bs Balances) Lock() {
	for _, b := range bs {
		if b == nil {
			continue
		}

		b.Lock()
	}
}

func (bs Balances) Unlock() {
	for _, b := range bs {
		if b == nil {
			continue
		}

		b.Unlock()
	}
}

func (bs BalanceSet) Lock() {
	for _, b := range bs {
		if len(b) == 0 {
			continue
		}

		b.Lock()
	}
}

func (bs BalanceSet) Unlock() {
	for _, b := range bs {
		if len(b) == 0 {
			continue
		}

		b.Unlock()
	}
}

func (bs *BalanceSet) Settle(ctx context.Context, requestTxID bitcoin.Hash32, requestIndex int,
	responseTxID bitcoin.Hash32, responseIndex int, now uint64) {

	for _, b := range *bs {
		if len(b) == 0 {
			continue
		}

		b.Settle(ctx, requestTxID, requestIndex, responseTxID, responseIndex, now)
	}
}

func (bs *BalanceSet) SettlePending(txid bitcoin.Hash32, index int, isMultiContract bool) {
	for _, b := range *bs {
		if len(b) == 0 {
			continue
		}

		b.SettlePending(txid, index, isMultiContract)
	}
}

func (bs *BalanceSet) CancelPending(txid bitcoin.Hash32, index int) {
	for _, b := range *bs {
		if len(b) == 0 {
			continue
		}

		b.CancelPending(txid, index)
	}
}

func (bs *BalanceSet) Revert(txid bitcoin.Hash32, index int) {
	for _, b := range *bs {
		if len(b) == 0 {
			continue
		}

		b.Revert(txid, index)
	}
}

func (bs *BalanceSet) LockingScripts() []bitcoin.Script {
	var result []bitcoin.Script
	for _, b := range *bs {
		if len(b) == 0 {
			continue
		}

		lockingScripts := b.LockingScripts()
		for _, ls := range lockingScripts {
			result = appendLockingScript(result, ls)
		}
	}

	return result
}

func appendLockingScript(lockingScripts []bitcoin.Script,
	lockingScript bitcoin.Script) []bitcoin.Script {
	for _, ls := range lockingScripts {
		if ls.Equal(lockingScript) {
			return lockingScripts
		}
	}

	return append(lockingScripts, lockingScript)
}
