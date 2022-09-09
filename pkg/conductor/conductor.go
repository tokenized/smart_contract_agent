package conductor

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

const (
	nextMesageIDPath = "conductor/next_message_id"
)

// Conductor is a factory and repository for smart contract agents.
type Conductor struct {
	baseKey       bitcoin.Key
	isTest        bool
	spyNodeClient spynode.Client

	contracts     *state.ContractCache
	balances      *state.BalanceCache
	transactions  *state.TransactionCache
	subscriptions *state.SubscriptionCache

	lookup map[state.ContractID]bitcoin.Hash32

	nextSpyNodeMessageID uint64

	lock sync.Mutex
}

func NewConductor(baseKey bitcoin.Key, isTest bool, spyNodeClient spynode.Client,
	contracts *state.ContractCache, balances *state.BalanceCache,
	transactions *state.TransactionCache, subscriptions *state.SubscriptionCache) *Conductor {

	return &Conductor{
		baseKey:              baseKey,
		isTest:               isTest,
		spyNodeClient:        spyNodeClient,
		contracts:            contracts,
		balances:             balances,
		transactions:         transactions,
		subscriptions:        subscriptions,
		lookup:               make(map[state.ContractID]bitcoin.Hash32),
		nextSpyNodeMessageID: 1,
	}
}

func (c *Conductor) IsTest() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.isTest
}

func (c *Conductor) NextSpyNodeMessageID() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.nextSpyNodeMessageID
}

func (c *Conductor) UpdateNextSpyNodeMessageID(id uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.nextSpyNodeMessageID = id + 1
}

func (c *Conductor) Load(ctx context.Context, store storage.StreamStorage) error {
	lookups, err := c.contracts.List(ctx, store)
	if err != nil {
		return errors.Wrap(err, "list contracts")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.lookup = make(map[state.ContractID]bitcoin.Hash32)
	for _, lookup := range lookups {
		contractID := state.CalculateContractID(lookup.LockingScript)
		c.lookup[contractID] = lookup.KeyHash
	}

	if b, err := store.Read(ctx, nextMesageIDPath); err != nil {
		if errors.Cause(err) != storage.ErrNotFound {
			return errors.Wrap(err, "read next message id")
		} else {
			c.nextSpyNodeMessageID = 1
		}
	} else {
		if err := binary.Read(bytes.NewReader(b), binary.LittleEndian,
			&c.nextSpyNodeMessageID); err != nil {
			return errors.Wrap(err, "next message id")
		}
	}

	return nil
}

func (c *Conductor) Save(ctx context.Context, store storage.StreamStorage) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	w := &bytes.Buffer{}
	if err := binary.Write(w, binary.LittleEndian, c.nextSpyNodeMessageID); err != nil {
		return errors.Wrap(err, "next message id")
	}

	if err := store.Write(ctx, nextMesageIDPath, w.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (c *Conductor) AddAgent(ctx context.Context,
	deriviationHash bitcoin.Hash32) (*agents.Agent, error) {

	c.lock.Lock()
	baseKey := c.baseKey
	c.lock.Unlock()

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

	addedContract, err := c.contracts.Add(ctx, contract)
	if err != nil {
		return nil, errors.Wrap(err, "add contract")
	}

	if addedContract == contract {
		c.lock.Lock()
		c.lookup[contractID] = deriviationHash
		c.lock.Unlock()

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

	agent, err := agents.NewAgent(key, addedContract, c.contracts, c.balances, c.transactions,
		c.subscriptions)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
}

func (c *Conductor) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*agents.Agent, error) {

	contractID := state.CalculateContractID(lockingScript)

	c.lock.Lock()
	deriviationHash, exists := c.lookup[contractID]
	baseKey := c.baseKey
	c.lock.Unlock()

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

	contract, err := c.contracts.Get(ctx, lockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get contract")
	}

	if contract == nil {
		return nil, nil
	}

	agent, err := agents.NewAgent(key, contract, c.contracts, c.balances, c.transactions,
		c.subscriptions)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
}

func (c *Conductor) ReleaseAgent(ctx context.Context, agent *agents.Agent) {
	agent.Release(ctx)
}
