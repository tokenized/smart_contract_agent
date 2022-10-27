package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

var (
	endian = binary.LittleEndian

	isTestLock sync.Mutex
	isTest     = true
)

type Caches struct {
	Contracts     *ContractCache
	Instruments   *InstrumentCache
	Balances      *BalanceCache
	Transactions  *TransactionCache
	Subscriptions *SubscriptionCache
	Services      *ContractServicesCache
	Votes         *VoteCache
}

func NewCaches(cache *cacher.Cache) (*Caches, error) {
	result := &Caches{}

	contracts, err := NewContractCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "contracts")
	}
	result.Contracts = contracts

	instruments, err := NewInstrumentCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "instruments")
	}
	result.Instruments = instruments

	balances, err := NewBalanceCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "balances")
	}
	result.Balances = balances

	transactions, err := NewTransactionCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "transactions")
	}
	result.Transactions = transactions

	subscriptions, err := NewSubscriptionCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "subscriptions")
	}
	result.Subscriptions = subscriptions

	services, err := NewContractServicesCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "services")
	}
	result.Services = services

	votes, err := NewVoteCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "votes")
	}
	result.Votes = votes

	return result, nil
}

type ContractHash bitcoin.Hash32

type InstrumentCode bitcoin.Hash20

func SetIsTest(value bool) {
	isTestLock.Lock()
	isTest = value
	isTestLock.Unlock()
}

func IsTest() bool {
	isTestLock.Lock()
	value := isTest
	isTestLock.Unlock()
	return value
}

func CalculateContractHash(lockingScript bitcoin.Script) ContractHash {
	return ContractHash(sha256.Sum256(lockingScript))
}

func LockingScriptHash(lockingScript bitcoin.Script) bitcoin.Hash32 {
	return bitcoin.Hash32(sha256.Sum256(lockingScript))
}

func appendHashIfDoesntExist(list []bitcoin.Hash32, value bitcoin.Hash32) []bitcoin.Hash32 {
	for _, v := range list {
		if v.Equal(&value) {
			return list // already contains value
		}
	}

	return append(list, value) // add value
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

func (id InstrumentCode) Equal(other InstrumentCode) bool {
	return bytes.Equal(id[:], other[:])
}

func (id InstrumentCode) Bytes() []byte {
	return id[:]
}

func (id InstrumentCode) String() string {
	return bitcoin.Hash20(id).String()
}

func (id InstrumentCode) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

func (id *InstrumentCode) UnmarshalText(text []byte) error {
	h, err := bitcoin.NewHash20FromStr(string(text))
	if err != nil {
		return err
	}

	*id = InstrumentCode(*h)
	return nil
}

func CopyRecursive(ctx context.Context, store storage.CopyList, fromPrefix, toPrefix string) error {
	items, err := store.List(ctx, fromPrefix)
	if err != nil {
		return errors.Wrap(err, "list")
	}

	println("Listed", len(items), "items")

	fromPrefixLength := len(fromPrefix)

	for i, from := range items {
		to := toPrefix + from[fromPrefixLength:]
		println("copy from", from, to)
		if err := store.Copy(ctx, from, to); err != nil {
			return errors.Wrapf(err, "copy %d/%d", i, len(items))
		}
	}

	return nil
}
