package firm

import (
	"context"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
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

	lock sync.Mutex
}

func NewFirm(baseKey bitcoin.Key, isTest bool, spyNodeClient spynode.Client,
	contracts *state.ContractCache, balances *state.BalanceCache,
	transactions *state.TransactionCache) *Firm {

	return &Firm{
		baseKey:       baseKey,
		isTest:        isTest,
		spyNodeClient: spyNodeClient,
		contracts:     contracts,
		balances:      balances,
		transactions:  transactions,
		lookup:        make(map[state.ContractID]bitcoin.Hash32),
	}
}

func (f *Firm) IsTest() bool {
	f.lock.Lock()
	defer f.lock.Unlock()

	return f.isTest
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

	return nil
}

func (f *Firm) GetAgent(ctx context.Context, lockingScript bitcoin.Script) (*agents.Agent, error) {
	f.lock.Lock()
	contractID := state.CalculateContractID(lockingScript)
	deriviationHash, exists := f.lookup[contractID]
	if !exists {
		f.lock.Unlock()
		return nil, nil
	}
	baseKey := f.baseKey
	f.lock.Unlock()

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
