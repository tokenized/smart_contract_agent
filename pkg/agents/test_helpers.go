package agents

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

type TestCaches struct {
	state.TestCaches

	Transactions *transactions.TransactionCache
	Services     *contract_services.ContractServicesCache
}

type TestData struct {
	store               *storage.MockStorage
	broadcaster         *state.MockTxBroadcaster
	caches              *TestCaches
	locker              *locker.InlineLocker
	peerChannelsFactory *peer_channels.Factory

	peerChannelResponder         *PeerChannelResponder
	peerChannelResponsesComplete chan interface{}
	peerChannelResponses         chan PeerChannelResponse

	schedulerInterrupt chan interface{}
	schedulerComplete  chan interface{}
	scheduler          *scheduler.Scheduler
	headers            *state.MockHeaders

	contractKey           bitcoin.Key
	contractLockingScript bitcoin.Script
	adminKey              bitcoin.Key
	adminLockingScript    bitcoin.Script
	feeLockingScript      bitcoin.Script

	oracleKey bitcoin.Key

	contract   *state.Contract
	instrument *state.Instrument

	mockStore *MockStore

	agentData AgentData
	agent     *Agent
}

func StartTestData(ctx context.Context, t testing.TB) *TestData {
	test := prepareTestData(ctx, t)

	return test
}

func StartTestAgent(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.contractKey, test.contractLockingScript, _ = state.MockKey()
	test.adminKey, test.adminLockingScript, _ = state.MockKey()

	test.contract = &state.Contract{
		LockingScript: test.contractLockingScript,
	}

	var err error
	test.contract, err = test.caches.Caches.Contracts.Add(ctx, test.contract)
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}
	_, test.feeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithContract(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.contractKey, test.contractLockingScript, test.adminKey, test.adminLockingScript, test.contract = state.MockContract(ctx,
		&test.caches.TestCaches)
	_, test.feeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrument(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.contractKey, test.contractLockingScript, test.adminKey, test.adminLockingScript, test.contract, test.instrument = state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, test.feeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithInstrumentWithOracle(ctx context.Context, t testing.TB) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.contractKey, test.contractLockingScript, test.adminKey, test.adminLockingScript, test.contract, test.instrument, test.oracleKey = MockInstrumentWithOracle(ctx,
		test.caches)
	_, test.feeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func StartTestAgentWithVoteSystems(ctx context.Context, t testing.TB,
	votingSystems []*actions.VotingSystemField) (*Agent, *TestData) {
	test := prepareTestData(ctx, t)

	test.contractKey, test.contractLockingScript, test.adminKey, test.adminLockingScript, test.contract = state.MockContractWithVoteSystems(ctx,
		&test.caches.TestCaches, votingSystems)
	_, test.feeLockingScript, _ = state.MockKey()

	finalizeTestAgent(ctx, t, test)

	return test.agent, test
}

func prepareTestData(ctx context.Context, t testing.TB) *TestData {
	test := &TestData{
		store:                        storage.NewMockStorage(),
		broadcaster:                  state.NewMockTxBroadcaster(),
		locker:                       locker.NewInlineLocker(),
		peerChannelsFactory:          peer_channels.NewFactory(),
		peerChannelResponsesComplete: make(chan interface{}),
		peerChannelResponses:         make(chan PeerChannelResponse),
		schedulerInterrupt:           make(chan interface{}),
		schedulerComplete:            make(chan interface{}),
		headers:                      state.NewMockHeaders(),
	}

	test.scheduler = scheduler.NewScheduler(test.broadcaster)

	test.caches = StartTestCaches(ctx, t, test.store, time.Second)

	if test.caches.Transactions == nil {
		t.Fatalf("Transactions is nil")
	}

	test.mockStore = NewMockStore(DefaultConfig(), test.feeLockingScript, test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, test.broadcaster,
		nil, nil, test.scheduler, test.peerChannelsFactory, test.peerChannelResponses)

	test.peerChannelResponder = NewPeerChannelResponder(test.caches.Caches,
		test.peerChannelsFactory)

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
		ProcessResponses(ctx, test.peerChannelResponder, test.peerChannelResponses)
		close(test.peerChannelResponsesComplete)
	}()

	return test
}

func finalizeTestAgent(ctx context.Context, t testing.TB, test *TestData) {
	test.agentData = AgentData{
		Key:              test.contractKey,
		LockingScript:    test.contractLockingScript,
		FeeLockingScript: test.feeLockingScript,
		ContractFee:      100,
		IsActive:         true,
	}

	var err error
	test.agent, err = NewAgent(ctx, test.agentData, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store,
		test.broadcaster, nil, test.headers, test.scheduler, test.mockStore,
		test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}
}

func StopTestAgent(ctx context.Context, t *testing.T, test *TestData) {
	close(test.peerChannelResponses)
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

	if test.agent != nil {
		test.agent.Release(ctx)
	}
	if test.instrument != nil {
		test.caches.Caches.Instruments.Release(ctx, test.contractLockingScript,
			test.instrument.InstrumentCode)
	}
	if test.contract != nil {
		test.caches.Caches.Contracts.Release(ctx, test.contractLockingScript)
	}
	test.caches.StopTestCaches()
}

func StartTestCaches(ctx context.Context, t testing.TB, store storage.StreamStorage,
	timeout time.Duration) *TestCaches {

	tc := state.StartTestCaches(ctx, t, store, timeout)

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
	data []*AgentData

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

	lock sync.Mutex
}

func NewMockStore(config Config, feeLockingScript bitcoin.Script, caches *state.Caches,
	transactions *transactions.TransactionCache, services *contract_services.ContractServicesCache,
	locker locker.Locker, store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher,
	headers BlockHeaders, scheduler *scheduler.Scheduler,
	peerChannelsFactory *peer_channels.Factory,
	peerChannelResponses chan PeerChannelResponse) *MockStore {

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
	}
}

func (f *MockStore) Add(data AgentData) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.data = append(f.data, &data)
}

func (f *MockStore) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*Agent, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

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
		f.peerChannelResponses)
	if err != nil {
		return nil, errors.Wrap(err, "new agent")
	}

	return agent, nil
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

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &state.Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode,
		adminBalance)

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument,
		identityKey
}
