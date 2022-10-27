package state

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

type TestCaches struct {
	Timeout       time.Duration
	Cache         *cacher.Cache
	Caches        *Caches
	Interrupt     chan interface{}
	Complete      chan error
	Shutdown      chan error
	StartShutdown chan interface{}
	Wait          sync.WaitGroup

	failed     error
	failedLock sync.Mutex
}

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, t *testing.T, store storage.StreamStorage,
	config cacher.Config, timeout time.Duration) *TestCaches {

	result := &TestCaches{
		Timeout:       timeout,
		Cache:         cacher.NewCache(store, config),
		Interrupt:     make(chan interface{}),
		Complete:      make(chan error, 1),
		Shutdown:      make(chan error, 1),
		StartShutdown: make(chan interface{}),
	}

	var err error
	result.Caches, err = NewCaches(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create caches : %s", err))
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Cache panic : %s", err)
				result.Complete <- fmt.Errorf("panic: %s", err)
			}

			result.Wait.Done()
		}()

		result.Wait.Add(1)
		err := result.Cache.Run(ctx, result.Interrupt, result.Shutdown)
		if err != nil {
			t.Errorf("Cache returned an error : %s", err)
		}
		result.Complete <- err
	}()

	go func() {
		select {
		case err := <-result.Shutdown:
			t.Errorf("Cache shutting down : %s", err)

			if err != nil {
				result.failedLock.Lock()
				result.failed = err
				result.failedLock.Unlock()
			}

		case err, ok := <-result.Complete:
			if ok && err != nil {
				t.Errorf("Cache failed : %s", err)
			} else {
				// StartShutdown should have been triggered first.
				t.Errorf("Cache completed prematurely")
			}

		case <-result.StartShutdown:
			t.Logf("Cache start shutdown triggered")
		}
	}()

	return result
}

func (c *TestCaches) StopTestCaches() {
	close(c.StartShutdown)
	close(c.Interrupt)
	select {
	case err := <-c.Complete:
		if err != nil {
			panic(fmt.Sprintf("Cache failed : %s", err))
		}

	case <-time.After(c.Timeout):
		panic("Cache shutdown timed out")
	}
}

func (c *TestCaches) IsFailed() error {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()

	return c.failed
}

func MockContract(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract) {

	contractKey, contractLockingScript, _ := MockKey()
	adminKey, adminLockingScript, adminAddress := MockKey()
	_, _, entityAddress := MockKey()

	var keyHash bitcoin.Hash32
	rand.Read(keyHash[:])

	contract := &Contract{
		KeyHash:       keyHash,
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName:   "Test",
			AdminAddress:   adminAddress.Bytes(),
			ContractFee:    100,
			ContractType:   actions.ContractTypeInstrument,
			EntityContract: entityAddress.Bytes(),
			Timestamp:      uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
	}
	rand.Read(contract.FormationTxID[:])

	addedContract, err := caches.Caches.Contracts.Add(ctx, contract)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedContract != contract {
		panic("Created contract is not new")
	}

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract
}

func MockInstrumentOnly(ctx context.Context, caches *TestCaches,
	contractLockingScript, adminLockingScript bitcoin.Script) *Instrument {

	currency := &instruments.Currency{
		CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	instrument := &Instrument{
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
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

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &Balance{
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

	return instrument
}

// MockInstrument creates a contract and instrument.
// `caches.Contracts.Release(ctx, contractLockingScript)` must be called before the end of the test.
func MockInstrument(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract, *Instrument) {

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := MockContract(ctx, caches)

	currency := &instruments.Currency{
		CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	instrument := &Instrument{
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
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

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &Balance{
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

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument
}

// MockInstrument creates a contract and instrument.
// `caches.Contracts.Release(ctx, contractLockingScript)` must be called before the end of the test.
func MockInstrumentWithOracle(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract, *Instrument, bitcoin.Key) {

	identityContractAddress, identityKey := MockIdentityOracle(ctx, caches)

	contractKey, contractLockingScript, _ := MockKey()
	adminKey, adminLockingScript, adminAddress := MockKey()
	_, _, entityAddress := MockKey()

	var keyHash bitcoin.Hash32
	rand.Read(keyHash[:])

	contract := &Contract{
		KeyHash:       keyHash,
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

	instrument := &Instrument{
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
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
		instrument.InstrumentCode, &Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode, adminBalance)

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument, identityKey
}

func MockIdentityOracle(ctx context.Context,
	caches *TestCaches) (bitcoin.RawAddress, bitcoin.Key) {

	_, contractLockingScript, contractAddress := MockKey()
	_, _, adminAddress := MockKey()
	oracleKey, _, _ := MockKey()
	oraclePublicKey := oracleKey.PublicKey()

	var keyHash bitcoin.Hash32
	rand.Read(keyHash[:])

	contract := &Contract{
		KeyHash:       keyHash,
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

	if err := caches.Caches.Services.Update(ctx, contractLockingScript, contract.Formation,
		txid); err != nil {
		panic(fmt.Sprintf("Failed to update identity service : %s", err))
	}

	return contractAddress, oracleKey
}

func MockContractWithVoteSystems(ctx context.Context, caches *TestCaches,
	votingSystems []*actions.VotingSystemField) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract) {

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := MockContract(ctx, caches)

	contract.Formation.VotingSystems = votingSystems

	// Set all fields to be updatable by an administration proposal using these voting systems.
	permissions := permissions.Permissions{
		permissions.Permission{
			Permitted:              false, // Issuer can update field without proposal
			AdministrationProposal: true,  // Issuer can update field with a proposal
			HolderProposal:         false, // Holder's can initiate proposals to update field
		},
	}

	permissions[0].VotingSystemsAllowed = make([]bool, len(votingSystems))
	for i := range permissions[0].VotingSystemsAllowed {
		permissions[0].VotingSystemsAllowed[i] = true // Enable this voting system for proposals on this field.
	}

	permissionsBytes, err := permissions.Bytes()
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize contract permissions : %s", err))
	}
	contract.Formation.ContractPermissions = permissionsBytes

	contract.MarkModified()
	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract
}

func MockVoteContractAmendmentCompleted(ctx context.Context, caches *TestCaches,
	adminLockingScript, contractLockingScript bitcoin.Script, voteSystem uint32,
	amendments []*actions.AmendmentField) *Vote {

	now := uint64(time.Now().UnixNano())

	vote := &Vote{
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

	proposalScript, err := protocol.Serialize(vote.Proposal, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize proposal : %s", err))
	}
	proposalTx.AddTxOut(wire.NewTxOut(0, proposalScript))

	vote.ProposalTxID = proposalTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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

	voteTx := wire.NewMsgTx(1)
	voteTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 0), contractLockingScript))
	voteTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	voteScript, err := protocol.Serialize(vote.Vote, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize vote : %s", err))
	}
	voteTx.AddTxOut(wire.NewTxOut(0, voteScript))

	vote.VoteTxID = voteTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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

	vote.Result.VoteTxId = vote.VoteTxID[:]

	resultTx := wire.NewMsgTx(1)
	resultTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 1), contractLockingScript))
	resultTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	resultScript, err := protocol.Serialize(vote.Result, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize result : %s", err))
	}
	resultTx.AddTxOut(wire.NewTxOut(0, resultScript))

	vote.ResultTxID = resultTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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
	amendments []*actions.AmendmentField) *Vote {

	now := uint64(time.Now().UnixNano())

	vote := &Vote{
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

	proposalScript, err := protocol.Serialize(vote.Proposal, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize proposal : %s", err))
	}
	proposalTx.AddTxOut(wire.NewTxOut(0, proposalScript))

	vote.ProposalTxID = proposalTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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

	voteTx := wire.NewMsgTx(1)
	voteTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 0), contractLockingScript))
	voteTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	voteScript, err := protocol.Serialize(vote.Vote, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize vote : %s", err))
	}
	voteTx.AddTxOut(wire.NewTxOut(0, voteScript))

	vote.VoteTxID = voteTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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

	vote.Result.VoteTxId = vote.VoteTxID[:]

	resultTx := wire.NewMsgTx(1)
	resultTx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(vote.ProposalTxID, 1), contractLockingScript))
	resultTx.AddTxOut(wire.NewTxOut(200, contractLockingScript))

	resultScript, err := protocol.Serialize(vote.Result, IsTest())
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize result : %s", err))
	}
	resultTx.AddTxOut(wire.NewTxOut(0, resultScript))

	vote.ResultTxID = resultTx.TxHash()

	if _, err := caches.Caches.Transactions.AddExpandedTx(ctx, &expanded_tx.ExpandedTx{
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

	addedVote, err := caches.Caches.Votes.Add(ctx, contractLockingScript, vote)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedVote != vote {
		panic("Created vote is not new")
	}

	return vote
}

func MockKey() (bitcoin.Key, bitcoin.Script, bitcoin.RawAddress) {
	key, err := bitcoin.GenerateKey(bitcoin.MainNet)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate key : %s", err))
	}

	lockingScript, err := key.LockingScript()
	if err != nil {
		panic(fmt.Sprintf("Failed to create lockingScript : %s", err))
	}

	ra, err := key.RawAddress()
	if err != nil {
		panic(fmt.Sprintf("Failed to create raw address : %s", err))
	}

	return key, lockingScript, ra
}

func MockOutPoint(lockingScript bitcoin.Script, value uint64) *wire.OutPoint {
	outpoint := &wire.OutPoint{
		Index: uint32(rand.Intn(5)),
	}
	rand.Read(outpoint.Hash[:])

	return outpoint
}

type MockTxBroadcaster struct {
	txs []*wire.MsgTx

	lock sync.Mutex
}

func NewMockTxBroadcaster() *MockTxBroadcaster {
	return &MockTxBroadcaster{}
}

func (b *MockTxBroadcaster) BroadcastTx(ctx context.Context, tx *wire.MsgTx,
	indexes []uint32) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = append(b.txs, tx)
	return nil
}

func (b *MockTxBroadcaster) ClearTxs() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = nil
}

func (b *MockTxBroadcaster) GetLastTx() *wire.MsgTx {
	b.lock.Lock()
	defer b.lock.Unlock()

	l := len(b.txs)
	if l == 0 {
		return nil
	}

	return b.txs[l-1]
}

type MockHeaders struct {
	hashes  map[int]*bitcoin.Hash32
	headers map[int]*wire.BlockHeader

	lock sync.Mutex
}

func NewMockHeaders() *MockHeaders {
	return &MockHeaders{
		hashes:  make(map[int]*bitcoin.Hash32),
		headers: make(map[int]*wire.BlockHeader),
	}
}

func (h *MockHeaders) AddHash(height int, hash bitcoin.Hash32) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.hashes[height] = &hash
}

func (h *MockHeaders) AddHeader(height int, header *wire.BlockHeader) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.headers[height] = header
}

func (h *MockHeaders) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	hash, exists := h.hashes[height]
	if exists {
		return hash, nil
	}

	return nil, nil
}

func (h *MockHeaders) GetHeader(ctx context.Context, height int) (*wire.BlockHeader, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	header, exists := h.headers[height]
	if exists {
		return header, nil
	}

	return nil, nil
}
