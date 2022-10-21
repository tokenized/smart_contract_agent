package state

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/pkg/errors"
	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
)

var (
	endian = binary.LittleEndian

	isTestLock sync.Mutex
	isTest     = true
)

type Caches struct {
	Contracts     *ContractCache
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
