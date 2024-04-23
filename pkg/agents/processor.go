package agents

import (
	"context"
	"sync/atomic"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"

	"github.com/pkg/errors"
)

type TransactionProcessor struct {
	isTest atomic.Value
	store  Store
}

func NewTransactionProcessor(isTest bool, store Store) *TransactionProcessor {
	result := &TransactionProcessor{
		store: store,
	}

	result.isTest.Store(isTest)

	return result
}

func (p *TransactionProcessor) ProcessTransaction(ctx context.Context,
	transaction *transactions.Transaction) error {

	txid := transaction.GetTxID()
	isTest := p.isTest.Load().(bool)

	transaction.Lock()
	actionList, err := CompileActions(ctx, transaction, isTest)
	transaction.Unlock()
	if err != nil {
		if errors.Cause(err) == ErrInvalidAction {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", txid),
			}, "Failed to compile actions : %s", err)
			return nil
		}

		return errors.Wrap(err, "compile actions")
	}

	if len(actionList) == 0 {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
		}, "No relevant actions found")
	}

	var agentLockingScripts []bitcoin.Script
	for _, action := range actionList {
		for _, actionAgent := range action.Agents {
			agentLockingScripts = appendLockingScript(agentLockingScripts,
				actionAgent.LockingScript)
		}
	}

	for _, agentLockingScript := range agentLockingScripts {
		agent, err := p.store.GetAgent(ctx, agentLockingScript)
		if err != nil {
			return errors.Wrap(err, "get agent")
		}

		if agent == nil {
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Stringer("txid", txid),
				logger.Stringer("agent_locking_script", agentLockingScript),
			}, "Agent not in store")
			continue
		}
		defer p.store.Release(ctx, agent)

		if err := agent.UpdateTransaction(ctx, transaction, actionList); err != nil {
			return errors.Wrap(err, "update")
		}
	}

	return nil
}
