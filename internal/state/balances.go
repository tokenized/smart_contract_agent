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

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/smart_contract_agent/internal/cacher"
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

type balanceSet struct {
	PathID [2]byte `json:"path_id"`

	ContractLockingScript bitcoin.Script `json:"contract_locking_script"`
	InstrumentCode        InstrumentCode `json:"instrument_code"`

	Balances map[bitcoin.Hash32]*Balance `json:"balances"`

	isModified bool
	sync.Mutex
}

type balanceSets []*balanceSet

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
	typ := reflect.TypeOf(&balanceSet{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must be support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.CacheValue); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &BalanceCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *BalanceCache) Add(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balance *Balance) (*Balance, error) {

	hash, pathID := balanceSetPathID(balance.LockingScript)
	set := &balanceSet{
		PathID:                pathID,
		ContractLockingScript: contractLockingScript,
		InstrumentCode:        instrumentCode,
		Balances:              make(map[bitcoin.Hash32]*Balance),
	}
	set.Balances[hash] = balance

	item, err := c.cacher.Add(ctx, c.typ, set)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}
	set = item.(*balanceSet)

	set.Lock()
	result, exists := set.Balances[hash]

	if exists {
		set.Unlock()
		return result, nil
	}

	set.Balances[hash] = balance
	set.MarkModified()
	set.Unlock()

	return balance, nil
}

func (c *BalanceCache) AddMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances []*Balance) ([]*Balance, error) {

	var sets balanceSets
	for _, balance := range balances {
		sets.add(contractLockingScript, instrumentCode, balance)
	}

	values := make([]cacher.CacheValue, len(sets))
	for i, set := range sets {
		values[i] = set
	}

	items, err := c.cacher.AddMulti(ctx, c.typ, values)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	// Convert from items to sets
	sets = make(balanceSets, len(items))
	for i, item := range items {
		sets[i] = item.(*balanceSet)
	}

	// Build resulting balances from sets.
	result := make([]*Balance, len(balances))
	for i, balance := range balances {
		set := sets.getSet(contractLockingScript, instrumentCode, balance.LockingScript)
		if set == nil {
			// This shouldn't be possible if cacher.AddMulti is functioning properly.
			return nil, errors.New("Balance Set Missing") // balance set not within sets
		}
		hash, _ := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)

		set.Lock()
		gotBalance, exists := set.Balances[hash]
		if exists {
			result[i] = gotBalance
		} else {
			result[i] = balance
			set.Balances[hash] = balance
			set.isModified = true
		}
		set.Unlock()
	}

	return result, nil
}

func (c *BalanceCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScript bitcoin.Script) (*Balance, error) {

	hash, path := balanceSetPath(contractLockingScript, instrumentCode, lockingScript)

	item, err := c.cacher.Get(ctx, c.typ, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil // set doesn't exist
	}

	set := item.(*balanceSet)
	set.Lock()
	balance, exists := set.Balances[hash]
	set.Unlock()

	if !exists {
		c.cacher.Release(ctx, path)
		return nil, nil // balance not within set
	}

	return balance, nil
}

func (c *BalanceCache) GetMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScripts []bitcoin.Script) ([]*Balance, error) {

	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	paths := make([]string, len(lockingScripts))
	var getPaths []string
	for i, lockingScript := range lockingScripts {
		hash, path := balanceSetPath(contractLockingScript, instrumentCode, lockingScript)
		hashes[i] = hash
		paths[i] = path
		getPaths = appendStringIfDoesntExist(getPaths, path)
	}

	items, err := c.cacher.GetMulti(ctx, c.typ, getPaths)
	if err != nil {
		return nil, errors.Wrap(err, "get multi")
	}

	var sets balanceSets
	for _, item := range items {
		if item == nil {
			continue // a requested set must not exist
		}

		sets = append(sets, item.(*balanceSet))
	}

	result := make([]*Balance, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		set := sets.getSet(contractLockingScript, instrumentCode, lockingScript)
		if set == nil {
			c.cacher.Release(ctx, paths[i])
			continue
		}

		set.Lock()
		balance, exists := set.Balances[hashes[i]]
		set.Unlock()

		if !exists {
			c.cacher.Release(ctx, paths[i])
			continue // balance not within set
		}

		result[i] = balance
	}

	return result, nil
}

func (c *BalanceCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balance *Balance) error {

	balance.Lock()
	_, path := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)
	isModified := balance.isModified
	balance.Unlock()

	// Set set as modified
	if isModified {
		item, err := c.cacher.Get(ctx, c.typ, path)
		if err != nil {
			return errors.Wrap(err, "get")
		}

		if item == nil {
			return errors.New("Balance Set Missing")
		}

		set := item.(*balanceSet)
		set.Lock()
		set.isModified = true
		set.Unlock()

		c.cacher.Release(ctx, path) // release for get above
	}

	c.cacher.Release(ctx, path) // release for original request
	return nil
}

func (c *BalanceCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances []*Balance) error {

	var modifiedPaths, allPaths []string
	isModified := make([]bool, len(balances))
	for i, balance := range balances {
		balance.Lock()
		_, path := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)
		isModified[i] = balance.isModified
		balance.Unlock()

		allPaths = appendStringIfDoesntExist(allPaths, path)
		if isModified[i] {
			modifiedPaths = appendStringIfDoesntExist(modifiedPaths, path)
		}
	}

	modifiedItems, err := c.cacher.GetMulti(ctx, c.typ, modifiedPaths)
	if err != nil {
		return errors.Wrap(err, "get multi")
	}

	var sets balanceSets
	for _, item := range modifiedItems {
		if item == nil {
			continue // a requested set must not exist
		}

		sets = append(sets, item.(*balanceSet))
	}

	for i, balance := range balances {
		if !isModified[i] {
			continue
		}

		balance.Lock()
		lockingScript := balance.LockingScript
		balance.Unlock()

		set := sets.getSet(contractLockingScript, instrumentCode, lockingScript)
		if set == nil {
			return errors.New("Balance Set Missing")
		}

		set.MarkModified()
	}

	// Release from GetMulti above.
	for _, path := range modifiedPaths {
		c.cacher.Release(ctx, path)
	}

	// Release for balances specified in this function call.
	for _, path := range allPaths {
		c.cacher.Release(ctx, path)
	}

	return nil
}

func balanceSetPathID(lockingScript bitcoin.Script) (bitcoin.Hash32, [2]byte) {
	hash := sha256.Sum256(lockingScript)
	var firstTwoBytes [2]byte
	copy(firstTwoBytes[:], hash[:])
	return bitcoin.Hash32(hash), firstTwoBytes
}

func balanceHash(lockingScript bitcoin.Script) bitcoin.Hash32 {
	return bitcoin.Hash32(sha256.Sum256(lockingScript))
}

func balanceSetPath(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode,
	lockingScript bitcoin.Script) (bitcoin.Hash32, string) {
	hash, pathID := balanceSetPathID(lockingScript)
	return hash, fmt.Sprintf("%s/%s/%s/%x", balancePath, CalculateContractID(contractLockingScript),
		instrumentCode, pathID)
}

func (b *Balance) MarkModified() {
	b.isModified = true
}

func (b *Balance) IsModified() bool {
	return b.isModified
}

func (sets *balanceSets) add(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode,
	balance *Balance) {

	hash, pathID := balanceSetPathID(balance.LockingScript)

	for _, set := range *sets {
		set.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.InstrumentCode[:], instrumentCode[:]) {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.PathID[:], pathID[:]) {
			set.Unlock()
			continue
		}

		set.Balances[hash] = balance
		set.Unlock()
		return
	}

	set := &balanceSet{
		PathID:                pathID,
		ContractLockingScript: contractLockingScript,
		InstrumentCode:        instrumentCode,
		Balances:              make(map[bitcoin.Hash32]*Balance),
	}

	set.Lock()
	set.Balances[hash] = balance
	set.Unlock()
	*sets = append(*sets, set)
}

func (sets *balanceSets) getSet(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode,
	lockingScript bitcoin.Script) *balanceSet {

	_, pathID := balanceSetPathID(lockingScript)
	for _, set := range *sets {
		set.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.Unlock()
			continue
		}
		if !bytes.Equal(set.InstrumentCode[:], instrumentCode[:]) {
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

func (set *balanceSet) Path() string {
	return fmt.Sprintf("%s/%s/%s/%x", balancePath, CalculateContractID(set.ContractLockingScript),
		set.InstrumentCode, set.PathID)
}

func (set *balanceSet) MarkModified() {
	set.isModified = true
}

func (set *balanceSet) ClearModified() {
	set.isModified = false
}

func (set *balanceSet) IsModified() bool {
	return set.isModified
}

func (set *balanceSet) Serialize(w io.Writer) error {
	if err := binary.Write(w, endian, balanceVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if _, err := w.Write(set.PathID[:]); err != nil {
		return errors.Wrap(err, "path_id")
	}

	if err := writeString(w, set.ContractLockingScript); err != nil {
		return errors.Wrap(err, "contract")
	}

	if _, err := w.Write(set.InstrumentCode[:]); err != nil {
		return errors.Wrap(err, "instrument")
	}

	if err := binary.Write(w, endian, uint32(len(set.Balances))); err != nil {
		return errors.Wrap(err, "size")
	}

	for _, balance := range set.Balances {
		b, err := bsor.MarshalBinary(balance)
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

func (set *balanceSet) Deserialize(r io.Reader) error {
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

	if _, err := io.ReadFull(r, set.InstrumentCode[:]); err != nil {
		return errors.Wrap(err, "instrument")
	}

	var count uint32
	if err := binary.Read(r, endian, &count); err != nil {
		return errors.Wrap(err, "count")
	}

	set.Balances = make(map[bitcoin.Hash32]*Balance)
	for i := uint32(0); i < count; i++ {
		var size uint32
		if err := binary.Read(r, endian, &size); err != nil {
			return errors.Wrap(err, "size")
		}

		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			return errors.Wrap(err, "read")
		}

		balance := &Balance{}
		if _, err := bsor.UnmarshalBinary(b, balance); err != nil {
			return errors.Wrap(err, "unmarshal")
		}

		hash := bitcoin.Hash32(sha256.Sum256(balance.LockingScript))
		set.Balances[hash] = balance
	}

	return nil
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
