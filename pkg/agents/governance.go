package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processVote(ctx context.Context, transaction *state.Transaction,
	vote *actions.Vote, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing vote")

	return nil
}

func (a *Agent) processBallotCounted(ctx context.Context, transaction *state.Transaction,
	ballotCounted *actions.BallotCounted, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing ballot counted")

	return nil
}

func (a *Agent) processGovernanceResult(ctx context.Context, transaction *state.Transaction,
	result *actions.Result, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing governance result")

	return nil
}

// fetchReferenceVote fetches a vote for a reference txid provided in an amendment or modification.
// It returns nil if the txid is empty, or an error if the vote is not found or not complete.
func fetchReferenceVote(ctx context.Context, caches *state.Caches,
	contractLockingScript bitcoin.Script, txid []byte, isTest bool,
	now uint64) (*state.Vote, error) {

	if len(txid) == 0 {
		return nil, nil
	}

	refTxID, err := bitcoin.NewHash32(txid)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "RefTxID", now)
	}

	// Retrieve Vote Result
	voteResultTransaction, err := caches.Transactions.Get(ctx, *refTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get ref tx")
	}

	if voteResultTransaction == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result Tx Not Found", now)
	}
	defer caches.Transactions.Release(ctx, *refTxID)

	var voteResult *actions.Result
	voteResultTransaction.Lock()
	outputCount := voteResultTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := voteResultTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if r, ok := action.(*actions.Result); ok {
			voteResult = r
		}
	}
	voteResultTransaction.Unlock()

	if voteResult == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result Not Found", now)
	}

	// Retrieve Vote
	voteTxID, err := bitcoin.NewHash32(voteResult.VoteTxId)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: VoteTxId: %s", err), now)
	}

	// Retrieve the vote
	vote, err := caches.Votes.Get(ctx, contractLockingScript, *voteTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return nil, platform.NewRejectError(actions.RejectionsVoteNotFound,
			"RefTxID: Vote Result: Vote Data Not Found", now)
	}

	vote.Lock()

	if vote.Proposal == nil || vote.Result == nil {
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result: Vote Not Completed", now)
	}

	if vote.Result.Result != "A" {
		result := vote.Result.Result
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: Vote Not Accepted: %s", result), now)
	}

	vote.Unlock()
	return vote, nil
}

func validateVotingSystem(system *actions.VotingSystemField) error {
	if system.VoteType != "R" && system.VoteType != "A" && system.VoteType != "P" {
		return fmt.Errorf("Unsupported vote type : %s", system.VoteType)
	}
	if system.ThresholdPercentage == 0 || system.ThresholdPercentage >= 100 {
		return fmt.Errorf("Threshold Percentage out of range : %d", system.ThresholdPercentage)
	}
	if system.TallyLogic != 0 && system.TallyLogic != 1 {
		return fmt.Errorf("Tally Logic invalid : %d", system.TallyLogic)
	}
	return nil
}
