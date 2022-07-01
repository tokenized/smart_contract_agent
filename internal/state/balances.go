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
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/storage"
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
}

type balanceSet struct {
	PathID [2]byte `json:"path_id"`

	ContractLockingScript bitcoin.Script `json:"contract_locking_script"`
	InstrumentCode        InstrumentCode `json:"instrument_code"`

	Balances map[bitcoin.Hash32]*Balance `json:"balances"`

	lock sync.Mutex
}

type balanceSets []*balanceSet

type Balance struct {
	LockingScript bitcoin.Script  `bsor:"1" json:"locking_script"`
	Quantity      uint64          `bsor:"2" json:"quantity"`
	Timestamp     uint64          `bsor:"3" json:"timestamp"`
	TxID          *bitcoin.Hash32 `bsor:"4" json:"txID,omitempty"`

	Holdings []*BalanceHolding `bsor:"5" json:"holdings,omitempty"`

	sync.Mutex `bsor:"-"`
}

type BalanceHoldingCode byte

type BalanceHolding struct {
	Code BalanceHoldingCode `bsor:"1" json:"code,omitempty"`

	Expires  protocol.Timestamp `bsor:"2" json:"expires,omitempty"`
	Quantity uint64             `bsor:"3" json:"quantity,omitempty"`
	TxID     *bitcoin.Hash32    `bsor:"4" json:"txID,omitempty"`
}

func NewBalanceCache(store storage.StreamStorage, fetcherCount, expireCount int,
	expiration, fetchTimeout time.Duration) (*BalanceCache, error) {

	cacher, err := cacher.NewCache(store, reflect.TypeOf(&balanceSet{}), fetcherCount, expireCount,
		expiration, fetchTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "cacher")
	}

	return &BalanceCache{
		cacher: cacher,
	}, nil
}

func (c *BalanceCache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	return c.cacher.Run(ctx, interrupt)
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

	item, err := c.cacher.Add(ctx, set)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}
	set = item.(*balanceSet)

	set.lock.Lock()
	result, exists := set.Balances[hash]
	set.lock.Unlock()

	if exists {
		return result, nil
	}

	set.Balances[hash] = balance
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

	items, err := c.cacher.AddMulti(ctx, values)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	modified := make([]bool, len(sets))
	sets = make(balanceSets, len(items))
	for i, item := range items {
		sets[i] = item.(*balanceSet)
	}

	result := make([]*Balance, len(balances))
	for i, balance := range balances {
		set := sets.getSet(contractLockingScript, instrumentCode, balance.LockingScript)
		if set == nil {
			// This shouldn't be possible if cacher.AddMulti is functioning properly.
			return nil, errors.New("Balance Set Missing") // balance set not within sets
		}
		hash, _ := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)

		set.lock.Lock()
		gotBalance, exists := set.Balances[hash]
		if exists {
			result[i] = gotBalance
		} else {
			result[i] = balance
			set.Balances[hash] = balance
			modified[i] = true
		}
		set.lock.Unlock()
	}

	for i, set := range sets {
		if modified[i] { // a balance was added to and existing set.
			if err := c.cacher.Save(ctx, set); err != nil {
				return nil, errors.Wrap(err, "save set")
			}
		}
	}

	return result, nil
}

func (c *BalanceCache) Get(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScript bitcoin.Script) (*Balance, error) {

	hash, path := balanceSetPath(contractLockingScript, instrumentCode, lockingScript)

	item, err := c.cacher.Get(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil // set doesn't exist
	}

	set := item.(*balanceSet)
	set.lock.Lock()
	balance, exists := set.Balances[hash]
	set.lock.Unlock()

	if !exists {
		return nil, nil // balance not within set
	}

	return balance, nil
}

func (c *BalanceCache) GetMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScripts []bitcoin.Script) ([]*Balance, error) {

	hashes := make([]bitcoin.Hash32, len(lockingScripts))
	var paths []string
	for i, lockingScript := range lockingScripts {
		hash, path := balanceSetPath(contractLockingScript, instrumentCode, lockingScript)
		hashes[i] = hash
		paths = appendStringIfDoesntExist(paths, path)
	}

	items, err := c.cacher.GetMulti(ctx, paths)
	if err != nil {
		return nil, errors.Wrap(err, "get multi")
	}

	sets := make(balanceSets, len(items))
	for i, item := range items {
		if item == nil {
			continue // a requested set must not exist
		}

		sets[i] = item.(*balanceSet)
	}

	result := make([]*Balance, len(lockingScripts))
	for i, lockingScript := range lockingScripts {
		set := sets.getSet(contractLockingScript, instrumentCode, lockingScript)
		if set == nil {
			continue
		}

		set.lock.Lock()
		balance, exists := set.Balances[hashes[i]]
		set.lock.Unlock()

		if !exists {
			continue // balance not within set
		}

		result[i] = balance
	}

	return result, nil
}

func appendStringIfDoesntExist(list []string, value string) []string {
	for _, v := range list {
		if v == value {
			return list // already contains value
		}
	}

	return append(list, value) // add value
}

func (c *BalanceCache) Save(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balance *Balance) error {

	_, path := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)
	item, err := c.cacher.Get(ctx, path)
	if err != nil {
		return errors.Wrap(err, "get")
	}
	if item == nil {
		return fmt.Errorf("Not found to save: %s", path)
	}
	defer c.cacher.Release(ctx, path)

	return c.cacher.Save(ctx, item)
}

func (c *BalanceCache) Release(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, lockingScript bitcoin.Script) {
	_, path := balanceSetPath(contractLockingScript, instrumentCode, lockingScript)
	c.cacher.Release(ctx, path)
}

func (c *BalanceCache) ReleaseMulti(ctx context.Context, contractLockingScript bitcoin.Script,
	instrumentCode InstrumentCode, balances []*Balance) {
	for _, balance := range balances {
		_, path := balanceSetPath(contractLockingScript, instrumentCode, balance.LockingScript)
		c.cacher.Release(ctx, path)
	}
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

func (sets *balanceSets) add(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode,
	balance *Balance) {

	hash, pathID := balanceSetPathID(balance.LockingScript)

	for _, set := range *sets {
		set.lock.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.lock.Unlock()
			continue
		}
		if !bytes.Equal(set.InstrumentCode[:], instrumentCode[:]) {
			set.lock.Unlock()
			continue
		}
		if !bytes.Equal(set.PathID[:], pathID[:]) {
			set.lock.Unlock()
			continue
		}

		set.Balances[hash] = balance
		set.lock.Unlock()
		return
	}

	set := &balanceSet{
		PathID:                pathID,
		ContractLockingScript: contractLockingScript,
		InstrumentCode:        instrumentCode,
		Balances:              make(map[bitcoin.Hash32]*Balance),
	}

	set.lock.Lock()
	set.Balances[hash] = balance
	set.lock.Unlock()
	*sets = append(*sets, set)
}

func (sets *balanceSets) getSet(contractLockingScript bitcoin.Script, instrumentCode InstrumentCode,
	lockingScript bitcoin.Script) *balanceSet {

	_, pathID := balanceSetPathID(lockingScript)
	for _, set := range *sets {
		set.lock.Lock()
		if !set.ContractLockingScript.Equal(contractLockingScript) {
			set.lock.Unlock()
			continue
		}
		if !bytes.Equal(set.InstrumentCode[:], instrumentCode[:]) {
			set.lock.Unlock()
			continue
		}
		if !bytes.Equal(set.PathID[:], pathID[:]) {
			set.lock.Unlock()
			continue
		}

		set.lock.Unlock()
		return set
	}

	return nil
}

func (set *balanceSet) Path() string {
	set.lock.Lock()
	defer set.lock.Unlock()

	return fmt.Sprintf("%s/%s/%s/%x", balancePath, CalculateContractID(set.ContractLockingScript),
		set.InstrumentCode, set.PathID)
}

func (set *balanceSet) Serialize(w io.Writer) error {
	if err := binary.Write(w, endian, balanceVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	set.lock.Lock()
	defer set.lock.Unlock()

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

func writeString(w io.Writer, b []byte) error {
	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func readString(r io.Reader) ([]byte, error) {
	var s uint32
	if err := binary.Read(r, endian, &s); err != nil {
		return nil, errors.Wrap(err, "size")
	}

	b := make([]byte, s)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "value")
	}

	return b, nil
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
