package agents

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Proposal_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	votingSystems := []*actions.VotingSystemField{
		{
			Name:                    "Basic",
			VoteType:                "R", // Relative Threshold
			TallyLogic:              0,   // Standard
			ThresholdPercentage:     50,
			VoteMultiplierPermitted: false,
			HolderProposalFee:       0,
		},
	}

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContractWithVoteSystems(ctx,
		caches, votingSystems)

	instrument := state.MockInstrumentOnly(ctx, caches, contract)

	balances := state.MockBalances(ctx, caches, contract, instrument, 1000)

	_, feeLockingScript, _ := state.MockKey()
	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	proposal := &actions.Proposal{
		Type: 0, // contract
		// InstrumentType       string
		// InstrumentCode       []byte
		VoteSystem: 0,
		// ProposedAmendments   []*AmendmentField
		VoteOptions:         "AR",
		VoteMax:             1,
		ProposalDescription: "Add John Bitcoin as board member",
		// ProposalDocumentHash []byte
		VoteCutOffTimestamp: uint64(time.Now().Add(time.Hour).UnixNano()),
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	var spentOutputs []*expanded_tx.Output

	// Add input
	outpoint := state.MockOutPoint(adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output 1
	if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add contract output 1 : %s", err)
	}

	// Add contract output 2
	if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add contract output 2 : %s", err)
	}

	// Add action output
	proposalScript, err := protocol.Serialize(proposal, true)
	if err != nil {
		t.Fatalf("Failed to serialize proposal action : %s", err)
	}

	if err := tx.AddOutput(proposalScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add proposal action output : %s", err)
	}

	// Add funding
	fundingValue := uint64(500)
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, fundingValue)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         fundingValue,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, fundingValue); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{proposal}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find vote action
	var vote *actions.Vote
	for _, txout := range responseTx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Vote); ok {
			vote = a
		} else {
			if r, ok := action.(*actions.Rejection); ok {
				rejectData := actions.RejectionsData(r.RejectionCode)
				if rejectData != nil {
					t.Errorf("Reject label : %s", rejectData.Label)
				}

				js, _ := json.MarshalIndent(r, "", "  ")
				t.Logf("Rejection : %s", js)
			}
		}
	}

	if vote == nil {
		t.Fatalf("Missing vote action")
	}

	js, _ := json.MarshalIndent(vote, "", "  ")
	t.Logf("Vote : %s", js)

	voteTxID := *responseTx.TxHash()

	// Check that ballots exist.
	ballots, err := caches.Caches.Ballots.List(ctx, contractLockingScript, voteTxID)
	if err != nil {
		t.Fatalf("Failed to list ballots : %s", err)
	}

	if len(ballots) != len(balances) {
		t.Fatalf("Wrong ballot count : got %d, want %d", len(ballots), len(balances))
	}

	caches.Caches.Ballots.ReleaseMulti(ctx, contractLockingScript, voteTxID, ballots)

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Ballots_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	votingSystems := []*actions.VotingSystemField{
		{
			Name:                    "Basic",
			VoteType:                "R", // Relative Threshold
			TallyLogic:              0,   // Standard
			ThresholdPercentage:     50,
			VoteMultiplierPermitted: false,
			HolderProposalFee:       0,
		},
	}

	contractKey, contractLockingScript, _, _, contract := state.MockContractWithVoteSystems(ctx,
		caches, votingSystems)

	instrument := state.MockInstrumentOnly(ctx, caches, contract)

	balanceCount := 1000
	balances := state.MockBalances(ctx, caches, contract, instrument, balanceCount)
	balancesToVote := balances

	_, feeLockingScript, _ := state.MockKey()
	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	vote := state.MockProposal(ctx, caches, contract, 0)
	vote.Lock()
	voteTxID := *vote.VoteTxID
	vote.Prepare(ctx, caches.Caches, contract, votingSystems[0])
	tokenQuantity := vote.TokenQuantity
	vote.Unlock()

	votedQuantity := uint64(0)
	for {
		index := rand.Intn(len(balancesToVote))
		balance := balancesToVote[index]
		balancesToVote = append(balancesToVote[:index], balancesToVote[index+1:]...)

		ballotCast := &actions.BallotCast{
			VoteTxId: voteTxID.Bytes(),
			Vote:     "A",
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		var spentOutputs []*expanded_tx.Output

		// Add input
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add contract output 1
		if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
			t.Fatalf("Failed to add contract output 1 : %s", err)
		}

		// Add action output
		ballotCastScript, err := protocol.Serialize(ballotCast, true)
		if err != nil {
			t.Fatalf("Failed to serialize ballot cast action : %s", err)
		}

		if err := tx.AddOutput(ballotCastScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add ballot cast action output : %s", err)
		}

		// Add funding
		fundingValue := uint64(250)
		fundingKey, fundingLockingScript, _ := state.MockKey()
		fundingOutpoint := state.MockOutPoint(fundingLockingScript, fundingValue)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: fundingLockingScript,
			Value:         fundingValue,
		})

		if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, fundingValue); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		_, changeLockingScript, _ := state.MockKey()
		tx.SetChangeLockingScript(changeLockingScript, "")

		if _, err := tx.Sign([]bitcoin.Key{balance.Key, fundingKey}); err != nil {
			t.Fatalf("Failed to sign tx : %s", err)
		}

		t.Logf("Ballot cast tx : %s", tx.String(bitcoin.MainNet))
		txid := *tx.MsgTx.TxHash()

		addTransaction := &state.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		now := uint64(time.Now().UnixNano())
		if err := agent.Process(ctx, transaction, []actions.Action{ballotCast}, now); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		responseTx := broadcaster.GetLastTx()
		if responseTx == nil {
			t.Fatalf("No response tx")
		}

		t.Logf("Response Tx : %s", responseTx)

		// Find ballot counted action
		var ballotCounted *actions.BallotCounted
		for _, txout := range responseTx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.BallotCounted); ok {
				ballotCounted = a
			} else {
				if r, ok := action.(*actions.Rejection); ok {
					rejectData := actions.RejectionsData(r.RejectionCode)
					if rejectData != nil {
						t.Errorf("Reject label : %s", rejectData.Label)
					}

					js, _ := json.MarshalIndent(r, "", "  ")
					t.Fatalf("Rejection : %s", js)
				}
			}
		}

		if ballotCounted == nil {
			t.Fatalf("Missing BallotCounted action")
		}

		js, _ := json.MarshalIndent(ballotCounted, "", "  ")
		t.Logf("BallotCounted : %s", js)

		caches.Caches.Transactions.Release(ctx, txid)

		votedQuantity += balance.Quantity
		if votedQuantity > tokenQuantity/2 {
			break
		}
	}

	caches.Caches.Votes.Release(ctx, contractLockingScript, voteTxID)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
