package agents

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processVote(ctx context.Context, transaction TransactionWithOutputs,
	vote *actions.Vote) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
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

func (a *Agent) processBallotCounted(ctx context.Context, transaction TransactionWithOutputs,
	ballotCounted *actions.BallotCounted) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
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

func (a *Agent) processGovernanceResult(ctx context.Context, transaction TransactionWithOutputs,
	result *actions.Result) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
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
