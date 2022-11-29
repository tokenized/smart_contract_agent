package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

func (a *Agent) addRecoveryRequests(ctx context.Context, txid bitcoin.Hash32,
	requestActions []Action) (bool, error) {

	if len(requestActions) == 0 {
		return false, nil // no requests
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.config.RecoveryMode {
		return false, nil
	}

	recoveryTx := &state.RecoveryTransaction{
		TxID:          txid,
		OutputIndexes: make([]int, len(requestActions)),
	}

	for i, action := range requestActions {
		recoveryTx.OutputIndexes[i] = action.OutputIndex
	}

	newRecoveryTxs := &state.RecoveryTransactions{
		Transactions: []*state.RecoveryTransaction{recoveryTx},
	}

	recoveryTxs, err := a.caches.RecoveryTransactions.Add(ctx, a.lockingScript, newRecoveryTxs)
	if err != nil {
		return false, errors.Wrap(err, "get recovery txs")
	}
	defer a.caches.RecoveryTransactions.Release(ctx, a.lockingScript)

	if recoveryTxs != newRecoveryTxs {
		recoveryTxs.Lock()
		recoveryTxs.Append(recoveryTx)
		recoveryTxs.Unlock()
	}

	return true, nil
}

func (a *Agent) removeRecoveryRequest(ctx context.Context, txid bitcoin.Hash32,
	outputIndex int) (bool, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.config.RecoveryMode {
		return false, nil
	}

	recoveryTxs, err := a.caches.RecoveryTransactions.Get(ctx, a.lockingScript)
	if err != nil {
		return false, errors.Wrap(err, "get recovery txs")
	}

	if recoveryTxs == nil {
		return false, nil
	}
	defer a.caches.RecoveryTransactions.Release(ctx, a.lockingScript)

	recoveryTxs.Lock()
	result := recoveryTxs.RemoveOutput(txid, outputIndex)
	recoveryTxs.Unlock()

	if result {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("request_txid", txid),
			logger.Int("request_output_index", outputIndex),
		}, "Removed recovery request")
	}

	return result, nil
}

func (a *Agent) ProcessRecoveryRequests(ctx context.Context, now uint64) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("recovery", uuid.New()))

	agentLockingScript := a.LockingScript()
	recoveryTxs, err := a.caches.RecoveryTransactions.Get(ctx, agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "get recovery txs")
	}

	if recoveryTxs == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "No recovery requests to process")
		return nil
	}
	defer a.caches.RecoveryTransactions.Release(ctx, agentLockingScript)

	recoveryTxs.Lock()
	copyTxs := recoveryTxs.Copy()
	recoveryTxs.Unlock()

	if len(copyTxs.Transactions) == 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "No recovery requests to process")
		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("contract_locking_script", agentLockingScript),
		logger.Int("request_count", len(copyTxs.Transactions)),
	}, "Processing recovery requests")

	for _, request := range copyTxs.Transactions {
		if err := a.processRecoveryRequest(ctx, request, now); err != nil {
			return errors.Wrapf(err, "process recovery request: %s", request.TxID)
		}

		recoveryTxs.Lock()
		recoveryTxs.Remove(request.TxID)
		recoveryTxs.Unlock()
	}

	return nil
}

func (a *Agent) processRecoveryRequest(ctx context.Context, request *state.RecoveryTransaction,
	now uint64) error {

	transaction, err := a.caches.Transactions.Get(ctx, request.TxID)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if transaction == nil {
		return errors.New("Transaction Not Found")
	}
	defer a.caches.Transactions.Release(ctx, request.TxID)

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()
	actionList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	var recoveryActions []Action
	for _, outputIndex := range request.OutputIndexes {
		found := false
		for _, action := range actionList {
			if outputIndex == action.OutputIndex {
				found = true
				recoveryActions = append(recoveryActions, action)
				break
			}
		}

		if !found {
			return fmt.Errorf("Output action %d not found", outputIndex)
		}
	}

	if err := a.Process(ctx, transaction, recoveryActions, now); err != nil {
		return errors.Wrap(err, "process")
	}

	return nil
}
