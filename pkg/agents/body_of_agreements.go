package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

func (a *Agent) processBodyOfAgreementOffer(ctx context.Context,
	transaction *state.Transaction, offer *actions.BodyOfAgreementOffer, now uint64) error {

	return errors.New("Not Implemented")
}

func (a *Agent) processBodyOfAgreementAmendment(ctx context.Context,
	transaction *state.Transaction, amendment *actions.BodyOfAgreementAmendment, now uint64) error {

	return errors.New("Not Implemented")
}

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context,
	transaction *state.Transaction, formation *actions.BodyOfAgreementFormation, now uint64) error {

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
	txid := transaction.GetTxID()
	a.contract.BodyOfAgreementFormationTxID = &txid
	a.contract.MarkModified()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(formation.Timestamp)),
	}, "Updated body of agreement formation")

	a.contract.Unlock()

	return nil
}
