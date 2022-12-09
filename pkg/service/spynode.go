package service

import (
	"context"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	spynode "github.com/tokenized/spynode/pkg/client"
)

func (s *Service) HandleTx(ctx context.Context, spyNodeTx *spynode.Tx) {
	txid := *spyNodeTx.Tx.TxHash()

	s.UpdateNextSpyNodeMessageID(spyNodeTx.ID)

	transaction, err := s.addTx(ctx, txid, spyNodeTx)
	if err != nil {
		logger.Error(ctx, "Failed to add tx : %s", err)
		return
	}
	defer s.caches.Transactions.Release(ctx, txid)

	isTest := s.agent.IsTest()
	transaction.Lock()
	outputCount := transaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if formation, ok := action.(*actions.ContractFormation); ok {
			// Get contract agent's locking script
			inputOutput, err := transaction.InputOutput(0)
			if err != nil {
				logger.Error(ctx, "Failed to get first input's output : %s", err)
				continue
			}

			if err := s.caches.Services.Update(ctx, inputOutput.LockingScript, formation,
				txid); err != nil {
				logger.Error(ctx, "Failed to update services : %s", err)
			}
		}
	}
	transaction.Unlock()

	if err := s.agent.UpdateTransaction(ctx, transaction); err != nil {
		logger.Error(ctx, "Failed to update tx : %s", err)
		return
	}
}

func (s *Service) HandleTxUpdate(ctx context.Context, txUpdate *spynode.TxUpdate) {
	transaction, err := s.caches.Transactions.Get(ctx, txUpdate.TxID)
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
	defer s.caches.Transactions.Release(ctx, txUpdate.TxID)

	transaction.Lock()

	if txUpdate.State.Safe {
		if transaction.State&wallet.TxStateSafe == 0 {
			transaction.State = wallet.TxStateSafe
			transaction.MarkModified()
		}
	} else {
		if txUpdate.State.UnSafe {
			if transaction.State&wallet.TxStateUnsafe == 0 {
				transaction.State |= wallet.TxStateUnsafe
				transaction.MarkModified()
			}
		}
		if txUpdate.State.Cancelled {
			if transaction.State&wallet.TxStateCancelled == 0 {
				transaction.State |= wallet.TxStateCancelled
				transaction.MarkModified()
			}
		}
	}

	if txUpdate.State.MerkleProof != nil {
		mp := txUpdate.State.MerkleProof.ConvertToMerkleProof(txUpdate.TxID)
		if transaction.AddMerkleProof(mp) {
			transaction.MarkModified()
		}
	}

	transaction.Unlock()

	if err := s.agent.UpdateTransaction(ctx, transaction); err != nil {
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txUpdate.TxID),
		}, "Failed to update tx : %s", err)
		return
	}
}

func (s *Service) HandleHeaders(ctx context.Context, headers *spynode.Headers) {
	// TODO Track headers for merkle proofs and to determine when RectifiedSettlement actions should
	// be sent. --ce

}

func (s *Service) HandleInSync(ctx context.Context) {

}

func (s *Service) HandleMessage(ctx context.Context, payload spynode.MessagePayload) {
	switch msg := payload.(type) {
	case *spynode.AcceptRegister:
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
			if err := spynode.SubscribeAddresses(ctx, []bitcoin.RawAddress{ra},
				s.spyNodeClient); err != nil {
				logger.Error(ctx, "Failed to subscribe to contract address : %s", err)
			}
		} else {
			logger.Error(ctx, "Failed to convert agent locking script to address : %s", err)
		}
	}
}
