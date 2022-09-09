package agents

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/logger"
	"github.com/tokenized/specification/dist/golang/actions"
)

func (a *Agent) processFreeze(ctx context.Context, transaction TransactionWithOutputs,
	freeze *actions.Freeze) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing freeze")

	return nil
}

func (a *Agent) processThaw(ctx context.Context, transaction TransactionWithOutputs,
	thaw *actions.Thaw) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing thaw")

	return nil
}

func (a *Agent) processConfiscation(ctx context.Context, transaction TransactionWithOutputs,
	confiscation *actions.Confiscation) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing confiscation")

	return nil
}

func (a *Agent) processReconciliation(ctx context.Context, transaction TransactionWithOutputs,
	reconciliation *actions.Reconciliation) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing reconciliation")

	return nil
}
