package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context,
	transaction TransactionWithOutputs, formation *actions.BodyOfAgreementFormation) error {

	// First input must be the agent's locking script
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing body of agreement formation")

	a.contract.Lock()

	if a.contract.BodyOfAgreementFormation != nil &&
		formation.Timestamp < a.contract.BodyOfAgreementFormation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("existing_timestamp",
				int64(a.contract.BodyOfAgreementFormation.Timestamp)),
		}, "Older body of agreement formation")
	}

	a.contract.BodyOfAgreementFormation = formation
	txid := transaction.TxID()
	a.contract.BodyOfAgreementFormationTxID = &txid
	a.contract.MarkModified()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(formation.Timestamp)),
	}, "Updated body of agreement formation")

	a.contract.Unlock()

	return nil
}
