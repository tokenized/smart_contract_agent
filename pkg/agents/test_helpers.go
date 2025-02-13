package agents

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	mathRand "math/rand"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/threads"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

type TestCaches struct {
	state.TestCaches

	Transactions *transactions.TransactionCache
	Services     *contract_services.ContractServicesCache
}

type TestData struct {
	Store               *storage.MockStorage
	Broadcaster         *state.MockTxBroadcaster
	Caches              *TestCaches
	Locker              *locker.InlineLocker
	PeerChannelsFactory *peer_channels.Factory

	peerChannelResponder         *PeerChannelResponder
	peerChannelResponsesComplete chan interface{}
	PeerChannelResponses         chan PeerChannelResponse

	schedulerInterrupt chan interface{}
	schedulerComplete  chan interface{}
	scheduler          *scheduler.Scheduler

	headers *state.MockHeaders

	statisticsInterrupt chan interface{}
	statisticsComplete  chan interface{}
	Statistics          *statistics.Processor

	triggerDependencyInterrupt chan interface{}
	triggerDependencyComplete  chan interface{}
	DependencyTrigger          *DependencyTrigger

	ContractKey           bitcoin.Key
	ContractLockingScript bitcoin.Script
	AdminKey              bitcoin.Key
	AdminLockingScript    bitcoin.Script
	FeeLockingScript      bitcoin.Script

	oracleKey bitcoin.Key

	Contract   *state.Contract
	Instrument *state.Instrument

	mockStore *MockStore

	agentData AgentData
	agent     *Agent
}

type TestAgentData struct {
	Agent *Agent

	Contract   *state.Contract
	Instrument *state.Instrument

	ContractKey           bitcoin.Key
	ContractLockingScript bitcoin.Script
	AdminKey              bitcoin.Key
	AdminLockingScript    bitcoin.Script
	FeeLockingScript      bitcoin.Script

	Caches      *TestCaches
	Broadcaster *state.MockTxBroadcaster
}

func (t *TestData) AddAgent(agent *Agent) {
	t.mockStore.AddAgent(agent)
}

func (t *TestAgentData) ProcessTx(ctx context.Context,
	etx *expanded_tx.ExpandedTx) []*expanded_tx.ExpandedTx {

	transaction, err := t.Caches.Transactions.AddExpandedTx(ctx, etx)
	if err != nil {
		panic(fmt.Sprintf("Failed to add transaction : %s", err))
	}
	defer t.Caches.Transactions.Release(ctx, etx.TxID())

	transaction.Lock()
	agentActions, err := CompileActions(ctx, transaction, true)
	transaction.State |= transactions.TxStateSafe
	transaction.Unlock()

	isRelevant := false
	for _, agentAction := range agentActions {
		if !agentAction.IsRelevant(t.Agent.LockingScript()) {
			continue
		}

		isRelevant = true
	}

	if !isRelevant {
		return nil
	}

	if err := t.Agent.UpdateTransaction(ctx, transaction, agentActions); err != nil {
		panic(fmt.Sprintf("Failed to process transaction : %s : %s", err, etx))
	}

	return t.Broadcaster.GetAndClearTxs()
}

func TestProcessTx(ctx context.Context, agents []*TestAgentData,
	etx *expanded_tx.ExpandedTx) []*expanded_tx.ExpandedTx {

	var responses []*expanded_tx.ExpandedTx
	currentTx := etx
	for {
		var currentResponseTx *expanded_tx.ExpandedTx
		for _, agent := range agents {
			responseTxs := agent.ProcessTx(ctx, currentTx)
			if len(responseTxs) > 0 {
				currentResponseTx = responseTxs[len(responseTxs)-1]
				responses = append(responses, responseTxs...)
			}
		}

		if currentResponseTx == nil {
			return responses
		}
		currentTx = currentResponseTx
	}
}

func TestProcessTxSingle(ctx context.Context, agents []*TestAgentData,
	etx *expanded_tx.ExpandedTx) []*expanded_tx.ExpandedTx {

	var responses []*expanded_tx.ExpandedTx
	for _, agent := range agents {
		responseTxs := agent.ProcessTx(ctx, etx)
		if len(responseTxs) > 0 {
			responses = append(responses, responseTxs...)
		}
	}

	return responses
}

func StartTestData(ctx context.Context, t testing.TB) *TestData {
	test := prepareTestData(ctx, t)

	return test
}

func StartTestDataWithCacher(ctx context.Context, t testing.TB, store *storage.MockStorage,
	cache cacher.Cacher) *TestData {

	test := prepareTestDataWithCacher(ctx, t, store, cache)

	return test
}

func StartTestAgent(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, _ = state.MockKey()
	test.AdminKey, test.AdminLockingScript, _ = state.MockKey()

	test.Contract = &state.Contract{
		LockingScript: test.ContractLockingScript,
	}

	var err error
	test.Contract, err = test.Caches.Caches.Contracts.Add(ctx, test.Contract)
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithContract(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract = state.MockContract(ctx,
		&test.Caches.TestCaches)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrument(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrumentWithContractFee(ctx context.Context, t testing.TB,
	contractFee uint64) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrumentWithContractFee(ctx,
		&test.Caches.TestCaches, contractFee)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithCacherWithInstrument(ctx context.Context, t testing.TB,
	store *storage.MockStorage, cache cacher.Cacher) (*Agent, *TestData) {

	test := prepareTestDataWithCacher(ctx, t, store, cache)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithCacherWithInstrumentTransferFee(ctx context.Context, t testing.TB,
	store *storage.MockStorage, cache cacher.Cacher, transferFee uint64,
	transferFeeLockingScript bitcoin.Script) (*Agent, *TestData) {

	test := prepareTestDataWithCacher(ctx, t, store, cache)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrumentWithTransferFee(ctx,
		&test.Caches.TestCaches, transferFee, transferFeeLockingScript)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithCacherWithInstrumentTransferFeeUsingInstrument(ctx context.Context, t testing.TB,
	store *storage.MockStorage, cache cacher.Cacher, transferFee uint64,
	transferFeeLockingScript bitcoin.Script) (*Agent, *TestData) {

	test := prepareTestDataWithCacher(ctx, t, store, cache)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrumentWithTransferFeeUsingInstrument(ctx,
		&test.Caches.TestCaches, transferFee, transferFeeLockingScript)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrumentCreditNote(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument = state.MockInstrumentCreditNote(ctx,
		&test.Caches.TestCaches)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrumentWithOracle(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract, test.Instrument, test.oracleKey = MockInstrumentWithOracle(ctx,
		test.Caches)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithVoteSystems(ctx context.Context, t testing.TB,
	votingSystems []*actions.VotingSystemField) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.ContractKey, test.ContractLockingScript, test.AdminKey, test.AdminLockingScript, test.Contract = state.MockContractWithVoteSystems(ctx,
		&test.Caches.TestCaches, votingSystems)
	_, test.FeeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func prepareTestData(ctx context.Context, t testing.TB) *TestData {
	test := &TestData{
		Store:                        storage.NewMockStorage(),
		Broadcaster:                  state.NewMockTxBroadcaster(),
		Locker:                       locker.NewInlineLocker(),
		PeerChannelsFactory:          peer_channels.NewFactory(),
		peerChannelResponsesComplete: make(chan interface{}),
		PeerChannelResponses:         make(chan PeerChannelResponse),
		schedulerInterrupt:           make(chan interface{}),
		schedulerComplete:            make(chan interface{}),
		statisticsInterrupt:          make(chan interface{}),
		statisticsComplete:           make(chan interface{}),
		triggerDependencyInterrupt:   make(chan interface{}),
		triggerDependencyComplete:    make(chan interface{}),
		headers:                      state.NewMockHeaders(),
	}

	test.scheduler = scheduler.NewScheduler(test.Broadcaster, time.Second)

	test.Caches = StartTestCaches(ctx, t, test.Store, time.Second)

	statProcessor, err := statistics.NewProcessor(test.Caches.Cache, 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to create statistics processor : %s", err)
	}
	test.Statistics = statProcessor

	if test.Caches.Transactions == nil {
		t.Fatalf("Transactions is nil")
	}

	test.DependencyTrigger = NewDependencyTrigger(2, time.Second, test.Caches.Transactions)

	test.mockStore = NewMockStore(DefaultConfig(), test.FeeLockingScript, test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, test.Broadcaster,
		nil, nil, test.scheduler, test.PeerChannelsFactory, test.PeerChannelResponses,
		test.Statistics, test.DependencyTrigger.Trigger)

	txProcessor := NewTransactionProcessor(true, test.mockStore)
	test.DependencyTrigger.SetTransactionProcessor(txProcessor.ProcessTransaction)

	test.peerChannelResponder = NewPeerChannelResponder(test.Caches.Caches,
		test.PeerChannelsFactory)

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Scheduler panic : %s", err)
			}
		}()

		if err := test.scheduler.Run(ctx, test.schedulerInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Scheduler returned an error : %s", err)
		}
		close(test.schedulerComplete)
	}()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Scheduler panic : %s", err)
			}
		}()

		if err := test.Statistics.Run(ctx, test.statisticsInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Statistics returned an error : %s", err)
		}
		close(test.statisticsComplete)
	}()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Dependency Trigger panic : %s", err)
			}
		}()

		if err := test.DependencyTrigger.Run(ctx, test.triggerDependencyInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Dependency Trigger returned an error : %s", err)
		}
		close(test.triggerDependencyComplete)
	}()

	go func() {
		ProcessResponses(ctx, test.peerChannelResponder, test.PeerChannelResponses)
		close(test.peerChannelResponsesComplete)
	}()

	return test
}

func prepareTestDataWithCacher(ctx context.Context, t testing.TB, store *storage.MockStorage,
	cache cacher.Cacher) *TestData {

	statProcessor, err := statistics.NewProcessor(cache, 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to create statistics processor : %s", err)
	}

	test := &TestData{
		Store:                        store,
		Broadcaster:                  state.NewMockTxBroadcaster(),
		Locker:                       locker.NewInlineLocker(),
		PeerChannelsFactory:          peer_channels.NewFactory(),
		peerChannelResponsesComplete: make(chan interface{}),
		PeerChannelResponses:         make(chan PeerChannelResponse),
		schedulerInterrupt:           make(chan interface{}),
		schedulerComplete:            make(chan interface{}),
		statisticsInterrupt:          make(chan interface{}),
		statisticsComplete:           make(chan interface{}),
		triggerDependencyInterrupt:   make(chan interface{}),
		triggerDependencyComplete:    make(chan interface{}),
		headers:                      state.NewMockHeaders(),
		Statistics:                   statProcessor,
	}

	test.scheduler = scheduler.NewScheduler(test.Broadcaster, time.Second)

	test.Caches = StartTestCachesWithCacher(ctx, t, cache, time.Second)

	if test.Caches.Transactions == nil {
		t.Fatalf("Transactions is nil")
	}

	agentConfig := DefaultConfig()
	agentConfig.MultiContractExpiration = config.NewDuration(time.Second)

	test.DependencyTrigger = NewDependencyTrigger(2, time.Second, test.Caches.Transactions)

	test.mockStore = NewMockStore(agentConfig, test.FeeLockingScript, test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, test.Broadcaster,
		nil, nil, test.scheduler, test.PeerChannelsFactory, test.PeerChannelResponses,
		test.Statistics, test.DependencyTrigger.Trigger)

	txProcessor := NewTransactionProcessor(true, test.mockStore)
	test.DependencyTrigger.SetTransactionProcessor(txProcessor.ProcessTransaction)

	test.peerChannelResponder = NewPeerChannelResponder(test.Caches.Caches,
		test.PeerChannelsFactory)

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Scheduler panic : %s : %s", err, string(debug.Stack()))
			}
		}()

		if err := test.scheduler.Run(ctx, test.schedulerInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Scheduler returned an error : %s", err)
		}
		close(test.schedulerComplete)
	}()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Statistics panic : %s : %s", err, string(debug.Stack()))
			}
		}()

		if err := test.Statistics.Run(ctx, test.statisticsInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Statistics returned an error : %s", err)
		}
		close(test.statisticsComplete)
	}()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Dependency Trigger panic : %s", err)
			}
		}()

		if err := test.DependencyTrigger.Run(ctx, test.triggerDependencyInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Dependency Trigger returned an error : %s", err)
		}
		close(test.triggerDependencyComplete)
	}()

	go func() {
		ProcessResponses(ctx, test.peerChannelResponder, test.PeerChannelResponses)
		close(test.peerChannelResponsesComplete)
	}()

	return test
}

func finalizeTestAgent(ctx context.Context, t testing.TB, test *TestData) {
	test.agentData = AgentData{
		Key:                test.ContractKey,
		LockingScript:      test.ContractLockingScript,
		FeeLockingScript:   test.FeeLockingScript,
		MinimumContractFee: 100,
		IsActive:           true,
	}

	var err error
	test.agent, err = NewAgent(ctx, test.agentData, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store,
		test.Broadcaster, nil, test.headers, test.scheduler, test.mockStore,
		test.PeerChannelsFactory, test.PeerChannelResponses, test.Statistics.Add,
		test.DependencyTrigger.Trigger)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}
}

func StopTestAgent(ctx context.Context, t *testing.T, test *TestData) {
	// Verify no txs are locked
	txs, err := test.Caches.Transactions.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list transactions : %s", err)
	}

	for _, tx := range txs {
		complete := make(chan interface{})
		go func() {
			tx.Lock()
			close(complete)
		}()

		select {
		case <-complete:
			txid := tx.TxID()
			tx.Unlock()
			test.Caches.Transactions.Release(ctx, txid)
			t.Logf("Tx was succussfully locked : %s", txid)
		case <-time.After(time.Millisecond * 10):
			t.Fatalf("Failed to lock tx : %s", tx.TxID())
		}
	}

	close(test.PeerChannelResponses)
	select {
	case <-test.peerChannelResponsesComplete:
	case <-time.After(time.Second):
		t.Fatalf("Peer channel response shut down timed out")
	}

	close(test.schedulerInterrupt)
	select {
	case <-test.schedulerComplete:
	case <-time.After(time.Second):
		t.Fatalf("Scheduler shut down timed out")
	}

	close(test.statisticsInterrupt)
	select {
	case <-test.statisticsComplete:
	case <-time.After(time.Second):
		t.Fatalf("Statistics shut down timed out")
	}

	close(test.triggerDependencyInterrupt)
	select {
	case <-test.triggerDependencyComplete:
	case <-time.After(time.Second):
		t.Fatalf("Dependency Trigger shut down timed out")
	}

	if test.agent != nil {
		test.agent.Release(ctx)
	}
	if test.Instrument != nil {
		test.Caches.Caches.Instruments.Release(ctx, test.ContractLockingScript,
			test.Instrument.InstrumentCode)
	}
	if test.Contract != nil {
		test.Caches.Caches.Contracts.Release(ctx, test.ContractLockingScript)
	}
	test.Caches.StopTestCaches()
}

func StartTestCaches(ctx context.Context, t testing.TB, store storage.StreamStorage,
	timeout time.Duration) *TestCaches {

	tc := state.StartTestCaches(ctx, t, cacher.NewSimpleCache(store), timeout)

	transactions, err := transactions.NewTransactionCache(tc.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transactions cache : %s", err))
	}

	services, err := contract_services.NewContractServicesCache(tc.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create services cache : %s", err))
	}

	return &TestCaches{
		TestCaches:   *tc,
		Transactions: transactions,
		Services:     services,
	}
}

func StartTestCachesWithCacher(ctx context.Context, t testing.TB, cache cacher.Cacher,
	timeout time.Duration) *TestCaches {

	tc := state.StartTestCaches(ctx, t, cache, timeout)

	transactions, err := transactions.NewTransactionCache(tc.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transactions cache : %s", err))
	}

	services, err := contract_services.NewContractServicesCache(tc.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create services cache : %s", err))
	}

	return &TestCaches{
		TestCaches:   *tc,
		Transactions: transactions,
		Services:     services,
	}
}

type MockStore struct {
	data   []*AgentData
	agents []*Agent

	config               Config
	feeLockingScript     bitcoin.Script
	caches               *state.Caches
	transactions         *transactions.TransactionCache
	services             *contract_services.ContractServicesCache
	locker               locker.Locker
	store                storage.CopyList
	broadcaster          Broadcaster
	fetcher              Fetcher
	headers              BlockHeaders
	scheduler            *scheduler.Scheduler
	peerChannelsFactory  *peer_channels.Factory
	peerChannelResponses chan PeerChannelResponse
	statistics           *statistics.Processor
	triggerDependency    TriggerDependency

	lock sync.Mutex
}

func NewMockStore(config Config, feeLockingScript bitcoin.Script, caches *state.Caches,
	transactions *transactions.TransactionCache, services *contract_services.ContractServicesCache,
	locker locker.Locker, store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher,
	headers BlockHeaders, scheduler *scheduler.Scheduler,
	peerChannelsFactory *peer_channels.Factory, peerChannelResponses chan PeerChannelResponse,
	statistics *statistics.Processor, triggerDependency TriggerDependency) *MockStore {

	return &MockStore{
		config:               config,
		caches:               caches,
		transactions:         transactions,
		services:             services,
		locker:               locker,
		store:                store,
		fetcher:              fetcher,
		broadcaster:          broadcaster,
		scheduler:            scheduler,
		peerChannelResponses: peerChannelResponses,
		peerChannelsFactory:  peerChannelsFactory,
		statistics:           statistics,
		triggerDependency:    triggerDependency,
	}
}

func (f *MockStore) Add(data AgentData) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.data = append(f.data, &data)
}

func (f *MockStore) AddAgent(agent *Agent) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.agents = append(f.agents, agent)
}

func (f *MockStore) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*Agent, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	for _, a := range f.agents {
		if a.LockingScript().Equal(lockingScript) {
			// Get from contracts so it can be released
			f.caches.Contracts.Get(ctx, lockingScript)
			return a, nil
		}
	}

	var data *AgentData
	for _, d := range f.data {
		if d.LockingScript.Equal(lockingScript) {
			data = d
			break
		}
	}

	if data == nil {
		return nil, nil
	}

	agent, err := NewAgent(ctx, *data, f.config, f.caches, f.transactions, f.services, f.locker,
		f.store, f.broadcaster, f.fetcher, f.headers, f.scheduler, f, f.peerChannelsFactory,
		f.peerChannelResponses, f.statistics.Add, f.triggerDependency)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}
	f.agents = append(f.agents, agent)

	return agent, nil
}

func (f *MockStore) Release(ctx context.Context, agent *Agent) {
	f.lock.Lock()
	defer f.lock.Unlock()

	lockingScript := agent.LockingScript()
	for i, a := range f.agents {
		if a.LockingScript().Equal(lockingScript) {
			f.agents = append(f.agents[:i], f.agents[i+1:]...)
			break
		}
	}

	agent.Release(ctx)
}

func MockVoteContractAmendmentCompleted(ctx context.Context, caches *TestCaches,
	adminLockingScript, contractLockingScript bitcoin.Script, voteSystem uint32,
	amendments []*actions.AmendmentField) *state.Vote {

	now := uint64(time.Now().UnixNano())

	vote := &state.Vote{
		Proposal: &actions.Proposal{
			Type: 0, // Referendum
			// InstrumentType       string
			// InstrumentCode       []byte
			VoteSystem:          voteSystem,
			ProposedAmendments:  amendments,
			VoteOptions:         "AR",
			VoteMax:             1,
			ProposalDescription: "Vote on amendments",
		},
		Vote: &actions.Vote{
			Timestamp: now - 1000,
		},
		Result: &actions.Result{
			// InstrumentType       string
			// InstrumentCode       []byte
			ProposedAmendments: amendments,
			OptionTally:        []uint64{100, 5},
			Result:             "A",
			Timestamp:          now - 1000,
		},
	}

	var fundingTxID bitcoin.Hash32
	rand.Read(fundingTxID[:])

	proposalTx := wire.NewMsgTx(1)
	proposalTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&fundingTxID, 0), nil))
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Vote
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Result

	proposalScript, err := protocol.Serialize(vote.Proposal, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize proposal : %s", err))
	}
	proposalTx.AddTxOut(wire.NewTxOut(0, proposalScript))

	vote.ProposalTxID = proposalTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: proposalTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: adminLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add proposal tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.ProposalTxID)

	voteTx := wire.NewMsgTx(1)
	voteTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 0), contractLockingScript))
	voteTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	voteScript, err := protocol.Serialize(vote.Vote, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize vote : %s", err))
	}
	voteTx.AddTxOut(wire.NewTxOut(0, voteScript))

	vote.VoteTxID = voteTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: voteTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: contractLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add vote tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.VoteTxID)

	vote.Result.VoteTxId = vote.VoteTxID[:]

	resultTx := wire.NewMsgTx(1)
	resultTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 1), contractLockingScript))
	resultTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	resultScript, err := protocol.Serialize(vote.Result, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize result : %s", err))
	}
	resultTx.AddTxOut(wire.NewTxOut(0, resultScript))

	vote.ResultTxID = resultTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: resultTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: contractLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add result tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.ResultTxID)

	addedVote, err := caches.Caches.Votes.Add(ctx, contractLockingScript, vote)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedVote != vote {
		panic("Created vote is not new")
	}

	return vote
}

func MockVoteInstrumentAmendmentCompleted(ctx context.Context, caches *TestCaches,
	instrumentType string, instrumentCode []byte, adminLockingScript,
	contractLockingScript bitcoin.Script, voteSystem uint32,
	amendments []*actions.AmendmentField) *state.Vote {

	now := uint64(time.Now().UnixNano())

	vote := &state.Vote{
		Proposal: &actions.Proposal{
			Type:                0, // Referendum
			InstrumentType:      instrumentType,
			InstrumentCode:      instrumentCode,
			VoteSystem:          voteSystem,
			ProposedAmendments:  amendments,
			VoteOptions:         "AR",
			VoteMax:             1,
			ProposalDescription: "Vote on amendments",
		},
		Vote: &actions.Vote{
			Timestamp: now - 1000,
		},
		Result: &actions.Result{
			InstrumentType:     instrumentType,
			InstrumentCode:     instrumentCode,
			ProposedAmendments: amendments,
			OptionTally:        []uint64{100, 5},
			Result:             "A",
			Timestamp:          now - 1000,
		},
	}

	var fundingTxID bitcoin.Hash32
	rand.Read(fundingTxID[:])

	proposalTx := wire.NewMsgTx(1)
	proposalTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&fundingTxID, 0), nil))
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Vote
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Result

	proposalScript, err := protocol.Serialize(vote.Proposal, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize proposal : %s", err))
	}
	proposalTx.AddTxOut(wire.NewTxOut(0, proposalScript))

	vote.ProposalTxID = proposalTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: proposalTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: adminLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add proposal tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.ProposalTxID)

	voteTx := wire.NewMsgTx(1)
	voteTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 0), contractLockingScript))
	voteTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	voteScript, err := protocol.Serialize(vote.Vote, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize vote : %s", err))
	}
	voteTx.AddTxOut(wire.NewTxOut(0, voteScript))

	vote.VoteTxID = voteTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: voteTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: contractLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add vote tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.VoteTxID)

	vote.Result.VoteTxId = vote.VoteTxID[:]

	resultTx := wire.NewMsgTx(1)
	resultTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 1), contractLockingScript))
	resultTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	resultScript, err := protocol.Serialize(vote.Result, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize result : %s", err))
	}
	resultTx.AddTxOut(wire.NewTxOut(0, resultScript))

	vote.ResultTxID = resultTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: resultTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: contractLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add result tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.ResultTxID)

	addedVote, err := caches.Caches.Votes.Add(ctx, contractLockingScript, vote)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedVote != vote {
		panic("Created vote is not new")
	}

	return vote
}

func MockProposal(ctx context.Context, caches *TestCaches, contract *state.Contract,
	voteSystemIndex uint32) *state.Vote {

	contract.Lock()

	contractLockingScript := contract.LockingScript

	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		panic(fmt.Sprintf("Failed to create admin address : %s", err))
	}

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		panic(fmt.Sprintf("Failed to create admin locking script : %s", err))
	}

	votingSystem := contract.Formation.VotingSystems[voteSystemIndex]

	contract.Unlock()

	now := uint64(time.Now().UnixNano())

	vote := &state.Vote{
		Proposal: &actions.Proposal{
			Type: 0, // Referendum
			// InstrumentType       string
			// InstrumentCode       []byte
			VoteSystem:          voteSystemIndex,
			VoteOptions:         "AR",
			VoteMax:             1,
			ProposalDescription: "Vote on something",
			VoteCutOffTimestamp: now + 2000000,
		},
		VotingSystem: votingSystem,
		Vote: &actions.Vote{
			Timestamp: now - 1000,
		},
	}

	var fundingTxID bitcoin.Hash32
	rand.Read(fundingTxID[:])

	proposalTx := wire.NewMsgTx(1)
	proposalTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&fundingTxID, 0), nil))
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Vote
	proposalTx.AddTxOut(wire.NewTxOut(200, contractLockingScript)) // For Result

	proposalScript, err := protocol.Serialize(vote.Proposal, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize proposal : %s", err))
	}
	proposalTx.AddTxOut(wire.NewTxOut(0, proposalScript))

	vote.ProposalTxID = proposalTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: proposalTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: adminLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add proposal tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.ProposalTxID)

	voteTx := wire.NewMsgTx(1)
	voteTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 0), contractLockingScript))
	voteTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	voteScript, err := protocol.Serialize(vote.Vote, true)
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize vote : %s", err))
	}
	voteTx.AddTxOut(wire.NewTxOut(0, voteScript))

	vote.VoteTxID = voteTx.TxHash()

	if _, err := caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
		Tx: voteTx,
		SpentOutputs: []*expanded_tx.Output{
			{
				Value:         200,
				LockingScript: contractLockingScript,
			},
		},
	}); err != nil {
		panic(fmt.Sprintf("Failed to add vote tx : %s", err))
	}
	caches.Transactions.Release(ctx, *vote.VoteTxID)

	addedVote, err := caches.Caches.Votes.Add(ctx, contractLockingScript, vote)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedVote != vote {
		panic("Created vote is not new")
	}

	return vote
}

func MockIdentityOracle(ctx context.Context,
	caches *TestCaches) (bitcoin.RawAddress, bitcoin.Key) {

	_, contractLockingScript, contractAddress := state.MockKey()
	_, _, adminAddress := state.MockKey()
	oracleKey, _, _ := state.MockKey()
	oraclePublicKey := oracleKey.PublicKey()

	contract := &state.Contract{
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName: "Test",
			AdminAddress: adminAddress.Bytes(),
			ContractFee:  100,
			ContractType: actions.ContractTypeEntity,
			Services: []*actions.ServiceField{
				{
					Type:      actions.ServiceTypeIdentityOracle,
					URL:       "mock://identity.id",
					PublicKey: oraclePublicKey.Bytes(),
				},
			},
			Timestamp: uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
	}
	rand.Read(contract.FormationTxID[:])

	var err error
	contract, err = caches.Caches.Contracts.Add(ctx, contract)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	caches.Caches.Contracts.Release(ctx, contractLockingScript)

	var txid bitcoin.Hash32
	rand.Read(txid[:])

	if err := caches.Services.Update(ctx, contractLockingScript, contract.Formation,
		txid); err != nil {
		panic(fmt.Sprintf("Failed to update identity service : %s", err))
	}

	return contractAddress, oracleKey
}

// MockInstrument creates a contract and instrument.
// `caches.Contracts.Release(ctx, contractLockingScript)` must be called before the end of the test.
func MockInstrumentWithOracle(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *state.Contract, *state.Instrument, bitcoin.Key) {

	identityContractAddress, identityKey := MockIdentityOracle(ctx, caches)

	contractKey, contractLockingScript, contractAddress := state.MockKey()
	adminKey, adminLockingScript, adminAddress := state.MockKey()
	_, _, entityAddress := state.MockKey()

	contract := &state.Contract{
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName:   "Test",
			AdminAddress:   adminAddress.Bytes(),
			ContractFee:    100,
			ContractType:   actions.ContractTypeInstrument,
			EntityContract: entityAddress.Bytes(),
			Oracles: []*actions.OracleField{
				{
					OracleTypes:    []uint32{actions.ServiceTypeIdentityOracle},
					EntityContract: identityContractAddress.Bytes(),
				},
			},
			Timestamp: uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
	}
	rand.Read(contract.FormationTxID[:])

	currency := &instruments.Currency{
		CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	contract.Lock()
	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++
	contract.MarkModified()
	contract.Unlock()

	instrument := &state.Instrument{
		InstrumentCode: state.InstrumentCode(nextInstrumentCode),
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
			InstrumentCode: nextInstrumentCode[:],
			// InstrumentPermissions            []byte   `protobuf:"bytes,3,opt,name=InstrumentPermissions,proto3" json:"InstrumentPermissions,omitempty"`
			// EnforcementOrdersPermitted       bool     `protobuf:"varint,6,opt,name=EnforcementOrdersPermitted,proto3" json:"EnforcementOrdersPermitted,omitempty"`
			// VotingRights                     bool     `protobuf:"varint,7,opt,name=VotingRights,proto3" json:"VotingRights,omitempty"`
			// VoteMultiplier                   uint32   `protobuf:"varint,8,opt,name=VoteMultiplier,proto3" json:"VoteMultiplier,omitempty"`
			// AdministrationProposal           bool     `protobuf:"varint,9,opt,name=AdministrationProposal,proto3" json:"AdministrationProposal,omitempty"`
			// HolderProposal                   bool     `protobuf:"varint,10,opt,name=HolderProposal,proto3" json:"HolderProposal,omitempty"`
			// InstrumentModificationGovernance uint32   `protobuf:"varint,11,opt,name=InstrumentModificationGovernance,proto3" json:"InstrumentModificationGovernance,omitempty"`
			AuthorizedTokenQty: authorizedQuantity,
			InstrumentType:     instruments.CodeCurrency,
			InstrumentPayload:  currencyBuf.Bytes(),
			// InstrumentRevision               uint32   `protobuf:"varint,15,opt,name=InstrumentRevision,proto3" json:"InstrumentRevision,omitempty"`
			Timestamp: uint64(time.Now().UnixNano()),
			// TradeRestrictions                []string `protobuf:"bytes,17,rep,name=TradeRestrictions,proto3" json:"TradeRestrictions,omitempty"`
		},
		CreationTxID: &bitcoin.Hash32{},
	}
	rand.Read(instrument.InstrumentCode[:])
	instrument.Creation.InstrumentCode = instrument.InstrumentCode[:]
	rand.Read(instrument.CreationTxID[:])
	copy(instrument.InstrumentType[:], []byte(instruments.CodeCurrency))

	addedInstrument, err := caches.Caches.Instruments.Add(ctx, contractLockingScript, instrument)
	if err != nil {
		panic(fmt.Sprintf("Failed to add instrument : %s", err))
	}

	if addedInstrument != instrument {
		panic("Created instrument is not new")
	}

	addedContract, err := caches.Caches.Contracts.Add(ctx, contract)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedContract != contract {
		panic("Created contract is not new")
	}

	balance := &state.Balance{
		LockingScript: adminLockingScript,
		Quantity:      authorizedQuantity,
		Timestamp:     instrument.Creation.Timestamp,
		TxID:          instrument.CreationTxID,
	}
	balance.Initialize()

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, balance)
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode,
		adminBalance)

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument,
		identityKey
}

func MockTransferTxWithReceivers(ctx context.Context, t testing.TB, test *TestData,
	contract *state.Contract, instrument *state.Instrument,
	receivers []bitcoin.Key) ([]bitcoin.Key, *expanded_tx.ExpandedTx) {

	balances := state.MockBalances(ctx, &test.Caches.TestCaches, contract, instrument,
		mathRand.Intn(5)+1)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(instrument.InstrumentType[:]),
		InstrumentCode: instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{
			instrumentTransfer,
		},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)
	var senderKeys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	quantity := uint64(0)
	for _, balance := range balances {
		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		quantity += balance.Quantity
		senderKeys = append(senderKeys, balance.Keys...)
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	// Add receivers
	var receiverKeys []bitcoin.Key
	remainingQuantity := quantity
	breakQuantity := quantity / 5
	for remainingQuantity > 0 {
		receivingQuantity := uint64(mathRand.Intn(int(breakQuantity))) + 1
		if receivingQuantity > remainingQuantity {
			receivingQuantity = remainingQuantity
		}
		remainingQuantity -= receivingQuantity

		key := receivers[0]
		receivers = receivers[1:]
		receiverKeys = append(receiverKeys, key)
		ra, _ := key.RawAddress()

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: receivingQuantity,
			})

		if len(receivers) == 0 {
			break
		}
	}

	for remainingQuantity > 0 {
		receivingQuantity := uint64(mathRand.Intn(int(breakQuantity))) + 1
		if receivingQuantity > remainingQuantity {
			receivingQuantity = remainingQuantity
		}
		remainingQuantity -= receivingQuantity

		key, _, ra := state.MockKey()
		receiverKeys = append(receiverKeys, key)

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: receivingQuantity,
			})
	}

	// Add contract outputs
	if err := tx.AddOutput(contract.LockingScript, 240, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	senderKeys = append(senderKeys, fundingKey)
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 1000)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         1000,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 1000); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(senderKeys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", txbuilder.TxString(tx))

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	transferTx := &expanded_tx.ExpandedTx{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	return receiverKeys, transferTx
}

func MockTransferTx(ctx context.Context, t testing.TB, test *TestData, contract *state.Contract,
	instrument *state.Instrument) ([]bitcoin.Key, *expanded_tx.ExpandedTx) {

	tokenTransferFee := uint64(0)
	instrument.Lock()
	if instrument.Creation.TransferFee != nil &&
		instrument.Creation.TransferFee.UseCurrentInstrument {
		tokenTransferFee = instrument.Creation.TransferFee.Quantity
	}
	instrument.Unlock()

	minimumBalance := tokenTransferFee + 5000
	balances := state.MockBalances(ctx, &test.Caches.TestCaches, contract, instrument,
		mathRand.Intn(5)+1)

	for {
		totalBalance := uint64(0)
		for _, balance := range balances {
			totalBalance += balance.Quantity
		}

		if totalBalance > minimumBalance {
			break
		}

		newBalances := state.MockBalances(ctx, &test.Caches.TestCaches, contract, instrument,
			mathRand.Intn(5)+1)
		balances = append(balances, newBalances...)
	}

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(instrument.InstrumentType[:]),
		InstrumentCode: instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{
			instrumentTransfer,
		},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)
	var senderKeys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	quantity := uint64(0)

	for _, balance := range balances {
		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		quantity += balance.Quantity
		senderKeys = append(senderKeys, balance.Keys...)
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	quantity -= tokenTransferFee

	// Add receivers
	var receiverKeys []bitcoin.Key
	remainingQuantity := quantity
	breakQuantity := quantity / 5
	for remainingQuantity > 0 {
		receivingQuantity := uint64(mathRand.Intn(int(breakQuantity))) + 1
		if receivingQuantity > remainingQuantity {
			receivingQuantity = remainingQuantity
		}
		remainingQuantity -= receivingQuantity

		key, _, ra := state.MockKey()
		receiverKeys = append(receiverKeys, key)

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: receivingQuantity,
			})
	}

	// Add contract outputs
	if err := tx.AddOutput(contract.LockingScript, 240, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	senderKeys = append(senderKeys, fundingKey)
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 1000)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         1000,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 1000); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(senderKeys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", txbuilder.TxString(tx))

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	transferTx := &expanded_tx.ExpandedTx{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	return receiverKeys, transferTx
}

func MockMultiContractTransferTx(ctx context.Context, t testing.TB, test *TestData,
	contract1, contract2 *state.Contract,
	instrument1, instrument2 *state.Instrument) ([]bitcoin.Key, []bitcoin.Key, *expanded_tx.ExpandedTx) {

	tokenTransferFee1 := uint64(0)
	instrument1.Lock()
	if instrument1.Creation.TransferFee != nil &&
		instrument1.Creation.TransferFee.UseCurrentInstrument {
		tokenTransferFee1 = instrument1.Creation.TransferFee.Quantity
	}
	instrument1.Unlock()
	minimumBalance1 := tokenTransferFee1 + 5000

	balances1 := state.MockBalances(ctx, &test.Caches.TestCaches, contract1, instrument1,
		mathRand.Intn(5)+1)

	for {
		totalBalance := uint64(0)
		for _, balance := range balances1 {
			totalBalance += balance.Quantity
		}

		if totalBalance > minimumBalance1 {
			break
		}

		newBalances := state.MockBalances(ctx, &test.Caches.TestCaches, contract1, instrument1,
			mathRand.Intn(5)+1)
		balances1 = append(balances1, newBalances...)
	}

	tokenTransferFee2 := uint64(0)
	instrument2.Lock()
	if instrument2.Creation.TransferFee != nil &&
		instrument2.Creation.TransferFee.UseCurrentInstrument {
		tokenTransferFee2 = instrument2.Creation.TransferFee.Quantity
	}
	instrument2.Unlock()
	minimumBalance2 := tokenTransferFee2 + 5000

	balances2 := state.MockBalances(ctx, &test.Caches.TestCaches, contract2, instrument2,
		mathRand.Intn(5)+1)

	for {
		totalBalance := uint64(0)
		for _, balance := range balances2 {
			totalBalance += balance.Quantity
		}

		if totalBalance > minimumBalance2 {
			break
		}

		newBalances := state.MockBalances(ctx, &test.Caches.TestCaches, contract2, instrument2,
			mathRand.Intn(5)+1)
		balances2 = append(balances2, newBalances...)
	}

	instrumentTransfer1 := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(instrument1.InstrumentType[:]),
		InstrumentCode: instrument1.InstrumentCode[:],
	}

	instrumentTransfer2 := &actions.InstrumentTransferField{
		ContractIndex:  1,
		InstrumentType: string(instrument2.InstrumentType[:]),
		InstrumentCode: instrument2.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{
			instrumentTransfer1,
			instrumentTransfer2,
		},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)
	var senderKeys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	quantity1 := uint64(0)
	quantity2 := uint64(0)

	for _, balance := range balances1 {
		instrumentTransfer1.InstrumentSenders = append(instrumentTransfer1.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		quantity1 += balance.Quantity
		senderKeys = append(senderKeys, balance.Keys...)
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	quantity1 -= tokenTransferFee1

	for _, balance := range balances2 {
		instrumentTransfer2.InstrumentSenders = append(instrumentTransfer2.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		quantity2 += balance.Quantity
		senderKeys = append(senderKeys, balance.Keys...)
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	quantity2 -= tokenTransferFee2

	// Add receivers
	var receiver1Keys []bitcoin.Key
	remainingQuantity := quantity1
	breakQuantity := quantity1 / 5
	for remainingQuantity > 0 {
		receivingQuantity := uint64(mathRand.Intn(int(breakQuantity))) + 1
		if receivingQuantity > remainingQuantity {
			receivingQuantity = remainingQuantity
		}
		remainingQuantity -= receivingQuantity

		key, _, ra := state.MockKey()
		receiver1Keys = append(receiver1Keys, key)

		instrumentTransfer1.InstrumentReceivers = append(instrumentTransfer1.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: receivingQuantity,
			})
	}

	var receiver2Keys []bitcoin.Key
	remainingQuantity = quantity2
	breakQuantity = quantity2 / 5
	for remainingQuantity > 0 {
		receivingQuantity := uint64(mathRand.Intn(int(breakQuantity))) + 1
		if receivingQuantity > remainingQuantity {
			receivingQuantity = remainingQuantity
		}
		remainingQuantity -= receivingQuantity

		key, _, ra := state.MockKey()
		receiver2Keys = append(receiver2Keys, key)

		instrumentTransfer2.InstrumentReceivers = append(instrumentTransfer2.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: receivingQuantity,
			})
	}

	// Add contract outputs
	if err := tx.AddOutput(contract1.LockingScript, 240, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}
	if err := tx.AddOutput(contract2.LockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add boomerang output
	if err := tx.AddOutput(contract1.LockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add boomerang output : %s", err)
	}

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	senderKeys = append(senderKeys, fundingKey)
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 1000)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         1000,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 1000); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(senderKeys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", txbuilder.TxString(tx))

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	transferTx := &expanded_tx.ExpandedTx{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	return receiver1Keys, receiver2Keys, transferTx
}
