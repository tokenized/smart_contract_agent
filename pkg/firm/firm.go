package firm

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

const (
	nextMesageIDPath = "firm/next_message_id"
)

// Firm is a factory and repository for smart contract agents.
type Firm struct {
	baseKey       bitcoin.Key
	isTest        bool
	spyNodeClient spynode.Client

	contracts    *state.ContractCache
	balances     *state.BalanceCache
	transactions *state.TransactionCache

	lookup map[state.ContractID]bitcoin.Hash32

	nextSpyNodeMessageID uint64

	lock sync.Mutex
}

func NewFirm(baseKey bitcoin.Key, isTest bool, spyNodeClient spynode.Client,
	contracts *state.ContractCache, balances *state.BalanceCache,
	transactions *state.TransactionCache) *Firm {

	return &Firm{
		baseKey:              baseKey,
		isTest:               isTest,
		spyNodeClient:        spyNodeClient,
		contracts:            contracts,
		balances:             balances,
		transactions:         transactions,
		lookup:               make(map[state.ContractID]bitcoin.Hash32),
		nextSpyNodeMessageID: 1,
	}
}

func (f *Firm) IsTest() bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.isTest
}

func (f *Firm) NextSpyNodeMessageID() uint64 {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.nextSpyNodeMessageID
}

func (f *Firm) UpdateNextSpyNodeMessageID(id uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.nextSpyNodeMessageID = id + 1
}

func (f *Firm) Load(ctx context.Context, store storage.StreamStorage) error {
	lookups, err := f.contracts.List(ctx, store)
	if err != nil {
		return errors.Wrap(err, "list contracts")
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.lookup = make(map[state.ContractID]bitcoin.Hash32)
	for _, lookup := range lookups {
		contractID := state.CalculateContractID(lookup.LockingScript)
		f.lookup[contractID] = lookup.KeyHash
	}

	if b, err := store.Read(ctx, nextMesageIDPath); err != nil {
		if errors.Cause(err) != storage.ErrNotFound {
			f.nextSpyNodeMessageID = 1
		}
	} else {
		if err := binary.Read(bytes.NewReader(b), binary.LittleEndian,
			&f.nextSpyNodeMessageID); err != nil {
			return errors.Wrap(err, "next message id")
		}
	}

	return nil
}

func (f *Firm) Save(ctx context.Context, store storage.StreamStorage) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	w := &bytes.Buffer{}
	if err := binary.Write(w, binary.LittleEndian, f.nextSpyNodeMessageID); err != nil {
		return errors.Wrap(err, "next message id")
	}

	if err := store.Write(ctx, nextMesageIDPath, w.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (f *Firm) AddAgent(ctx context.Context,
	deriviationHash bitcoin.Hash32) (*agents.Agent, error) {

	f.lock.Lock()
	baseKey := f.baseKey
	f.lock.Unlock()

	key, err := baseKey.AddHash(deriviationHash)
	if err != nil {
		return nil, errors.Wrap(err, "derive key")
	}

	lockingScript, err := key.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "locking script")
	}
	contractID := state.CalculateContractID(lockingScript)

	ra, err := key.RawAddress()
	if err != nil {
		return nil, errors.Wrap(err, "address")
	}

	contract := &state.Contract{
		KeyHash:       deriviationHash,
		LockingScript: lockingScript,
	}

	addedContract, err := f.contracts.Add(ctx, contract)
	if err != nil {
		return nil, errors.Wrap(err, "add contract")
	}

	if addedContract == contract {
		f.lock.Lock()
		f.lookup[contractID] = deriviationHash
		f.lock.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_id", contractID),
			logger.Stringer("address", bitcoin.NewAddressFromRawAddress(ra, bitcoin.MainNet)),
			logger.Stringer("locking_script", lockingScript),
		}, "Added new contract")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_id", contractID),
			logger.Stringer("address", bitcoin.NewAddressFromRawAddress(ra, bitcoin.MainNet)),
			logger.Stringer("locking_script", lockingScript),
		}, "Already have contract")
	}

	agent, err := agents.NewAgent(key, addedContract, f.contracts, f.balances, f.transactions)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
}

func (f *Firm) GetAgent(ctx context.Context, lockingScript bitcoin.Script) (*agents.Agent, error) {
	contractID := state.CalculateContractID(lockingScript)

	f.lock.Lock()
	deriviationHash, exists := f.lookup[contractID]
	baseKey := f.baseKey
	f.lock.Unlock()

	if !exists {
		return nil, nil
	}

	key, err := baseKey.AddHash(deriviationHash)
	if err != nil {
		return nil, errors.Wrap(err, "derive key")
	}

	derivedLockingScript, err := key.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "locking script")
	}

	if !lockingScript.Equal(derivedLockingScript) {
		return nil, errors.New("Wrong locking script")
	}

	contract, err := f.contracts.Get(ctx, lockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get contract")
	}

	if contract == nil {
		return nil, nil
	}

	agent, err := agents.NewAgent(key, contract, f.contracts, f.balances, f.transactions)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
}

func (f *Firm) ReleaseAgent(ctx context.Context, agent *agents.Agent) {
	agent.Release(ctx, f.contracts)
}
