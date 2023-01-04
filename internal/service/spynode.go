package service

import (
	"context"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	spynodeClient "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

func (s *Service) HandleTx(ctx context.Context, spyNodeTx *spynodeClient.Tx) {
	txid := *spyNodeTx.Tx.TxHash()

	transaction, err := s.addTx(ctx, txid, spyNodeTx)
	if err != nil {
		logger.Error(ctx, "Failed to add tx : %s", err)
		return
	}
	defer s.transactions.Release(ctx, txid)

	if err := s.handleTx(ctx, transaction); err != nil {
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", transaction.TxID()),
		}, "Failed to handle tx : %s", err)
	}

	s.UpdateNextSpyNodeMessageID(spyNodeTx.ID)
}

func (s *Service) HandleTxUpdate(ctx context.Context, txUpdate *spynodeClient.TxUpdate) {
	transaction, err := s.transactions.Get(ctx, txUpdate.TxID)
	if err != nil {
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txUpdate.TxID),
		}, "Failed to get tx : %s", err)
		return
	}

	if transaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txUpdate.TxID),
		}, "Update transaction not found")
		return
	}
	defer s.transactions.Release(ctx, txUpdate.TxID)

	isModified := false
	transaction.Lock()

	if txUpdate.State.Safe {
		if transaction.State&wallet.TxStateSafe == 0 {
			transaction.State = wallet.TxStateSafe
			transaction.MarkModified()
			isModified = true
		}
	} else {
		if txUpdate.State.UnSafe {
			if transaction.State&wallet.TxStateUnsafe == 0 {
				transaction.State |= wallet.TxStateUnsafe
				transaction.MarkModified()
				isModified = true
			}
		}
		if txUpdate.State.Cancelled {
			if transaction.State&wallet.TxStateCancelled == 0 {
				transaction.State |= wallet.TxStateCancelled
				transaction.MarkModified()
				isModified = true
			}
		}
	}

	if txUpdate.State.MerkleProof != nil {
		mp := txUpdate.State.MerkleProof.ConvertToMerkleProof(txUpdate.TxID)
		if transaction.AddMerkleProof(mp) {
			isModified = true
		}
	}

	transaction.Unlock()

	if !isModified {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", transaction.TxID()),
		}, "Tx state not modified : %s", err)
		s.UpdateNextSpyNodeMessageID(txUpdate.ID)
		return
	}

	if err := s.handleTx(ctx, transaction); err != nil {
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", transaction.TxID()),
		}, "Failed to handle tx : %s", err)
	}

	s.UpdateNextSpyNodeMessageID(txUpdate.ID)
}

func (s *Service) handleTx(ctx context.Context, transaction *transactions.Transaction) error {
	txid := transaction.GetTxID()
	isTest := s.agent.Config().IsTest
	transaction.Lock()
	actionList, err := agents.CompileActions(ctx, transaction, isTest)
	transaction.Unlock()
	if err != nil {
		if errors.Cause(err) == agents.ErrInvalidAction {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", txid),
			}, "Failed to compile actions : %s", err)
			return nil
		}

		return errors.Wrap(err, "compile actions")
	}

	agentLockingScript := s.agent.LockingScript()
	isRelevant := false
	for _, action := range actionList {
		if contractFormation, ok := action.Action.(*actions.ContractFormation); ok &&
			len(action.AgentLockingScripts) > 0 {
			if err := s.services.Update(ctx, action.AgentLockingScripts[0], contractFormation,
				txid); err != nil {
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.Stringer("txid", txid),
				}, "Failed to update services : %s", err)
			}
		}

		if action.IsRelevant(agentLockingScript) {
			isRelevant = true
		}
	}

	if isRelevant {
		if err := s.agent.UpdateTransaction(ctx, transaction, actionList); err != nil {
			return errors.Wrap(err, "update")
		}
	}

	return nil
}

func (s *Service) HandleHeaders(ctx context.Context, headers *spynodeClient.Headers) {
	// TODO Track headers for merkle proofs and to determine when RectifiedSettlement actions should
	// be sent. --ce

}

func (s *Service) HandleInSync(ctx context.Context) {

}

func (s *Service) HandleMessage(ctx context.Context, payload spynodeClient.MessagePayload) {
	switch msg := payload.(type) {
	case *spynodeClient.AcceptRegister:
		logger.Info(ctx, "Spynode registration accepted")

		if err := s.spyNodeClient.SubscribeContracts(ctx); err != nil {
			logger.Error(ctx, "Failed to subscribe to contracts : %s", err)
		}

		nextMessageID := s.NextSpyNodeMessageID()
		if err := s.spyNodeClient.Ready(ctx, nextMessageID); err != nil {
			logger.Error(ctx, "Failed to notify spynode ready : %s", err)
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("next_message", nextMessageID),
			logger.Uint64("message_count", msg.MessageCount),
		}, "Spynode client ready")

		lockingScript := s.agent.LockingScript()
		ra, err := bitcoin.RawAddressFromLockingScript(lockingScript)
		if err == nil {
			if err := spynodeClient.SubscribeAddresses(ctx, []bitcoin.RawAddress{ra},
				s.spyNodeClient); err != nil {
				logger.Error(ctx, "Failed to subscribe to contract address : %s", err)
			}
		} else {
			logger.Error(ctx, "Failed to convert agent locking script to address : %s", err)
		}
	}
}
