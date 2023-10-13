package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"

	"github.com/pkg/errors"
)

var (
	endian = binary.LittleEndian
)

type Caches struct {
	Contracts            *ContractCache
	Instruments          *InstrumentCache
	Balances             *BalanceCache
	Subscriptions        *SubscriptionCache
	Votes                *VoteCache
	Ballots              *BallotCache
	RecoveryTransactions *RecoveryTransactionsCache
	Responders           *ResponderCache
}

func NewCaches(cache cacher.Cacher) (*Caches, error) {
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

	subscriptions, err := NewSubscriptionCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "subscriptions")
	}
	result.Subscriptions = subscriptions

	votes, err := NewVoteCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "votes")
	}
	result.Votes = votes

	ballots, err := NewBallotCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "ballots")
	}
	result.Ballots = ballots

	recoveryTransactions, err := NewRecoveryTransactionsCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "recovery transactions")
	}
	result.RecoveryTransactions = recoveryTransactions

	responders, err := NewResponderCache(cache)
	if err != nil {
		return nil, errors.Wrap(err, "responders")
	}
	result.Responders = responders

	return result, nil
}

type ContractHash bitcoin.Hash32

type InstrumentCode bitcoin.Hash20

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

func (id ContractHash) Equal(other ContractHash) bool {
	return bytes.Equal(id[:], other[:])
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
