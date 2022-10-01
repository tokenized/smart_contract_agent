package agents

import (
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

func (a *Agent) processContractFormation(ctx context.Context, transaction *state.Transaction,
	formation *actions.ContractFormation, now uint64) error {

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

	logger.Info(ctx, "Processing contract formation")

	a.contract.Lock()

	isFirst := a.contract.Formation == nil

	if a.contract.Formation != nil && formation.Timestamp < a.contract.Formation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
		}, "Older contract formation")
	}

	a.contract.Formation = formation
	txid := transaction.GetTxID()
	a.contract.FormationTxID = &txid
	a.contract.MarkModified()

	if isFirst {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Initial contract formation")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Updated contract formation")
	}

	a.contract.Unlock()

	return nil
}
