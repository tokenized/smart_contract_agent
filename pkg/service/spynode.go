package service

import (
	"context"
	"time"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
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

	if err := s.agent.UpdateTransaction(ctx, transaction,
		uint64(time.Now().UnixNano())); err != nil {
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

	if err := s.agent.UpdateTransaction(ctx, transaction,
		uint64(time.Now().UnixNano())); err != nil {
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
	}
}
