package agents

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/cacher"
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

	"github.com/pkg/errors"
)

type TestCaches struct {
	state.TestCaches

	Transactions *transactions.TransactionCache
	Services     *contract_services.ContractServicesCache
}

func StartTestCaches(ctx context.Context, t *testing.T, store storage.StreamStorage,
	config cacher.Config, timeout time.Duration) *TestCaches {

	tc := state.StartTestCaches(ctx, t, store, config, timeout)

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
	keys []*bitcoin.Key

	config           Config
	feeLockingScript bitcoin.Script
	caches           *state.Caches
	transactions     *transactions.TransactionCache
	services         *contract_services.ContractServicesCache
	locker           locker.Locker
	store            storage.CopyList
	broadcaster      Broadcaster
	fetcher          Fetcher
	headers          BlockHeaders
	scheduler        *scheduler.Scheduler

	lock sync.Mutex
}

func NewMockStore(config Config, feeLockingScript bitcoin.Script, caches *state.Caches,
	transactions *transactions.TransactionCache, services *contract_services.ContractServicesCache,
	locker locker.Locker, store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher,
	headers BlockHeaders, scheduler *scheduler.Scheduler) *MockStore {

	return &MockStore{
		config:       config,
		caches:       caches,
		transactions: transactions,
		services:     services,
		locker:       locker,
		store:        store,
		fetcher:      fetcher,
		broadcaster:  broadcaster,
		scheduler:    scheduler,
	}
}

func (f *MockStore) AddKey(key bitcoin.Key) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.keys = append(f.keys, &key)
}

func (f *MockStore) GetAgent(ctx context.Context,
	lockingScript bitcoin.Script) (*Agent, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	var key *bitcoin.Key
	for _, k := range f.keys {
		ls, err := k.LockingScript()
		if err != nil {
			continue
		}

		if ls.Equal(lockingScript) {
			key = k
			break
		}
	}

	if key == nil {
		return nil, nil
	}

	agent, err := NewAgent(ctx, *key, lockingScript, f.config, f.feeLockingScript, f.caches,
		f.transactions, f.services, f.locker, f.store, f.broadcaster, f.fetcher, f.headers,
		f.scheduler, f, peer_channels.NewFactory())
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
