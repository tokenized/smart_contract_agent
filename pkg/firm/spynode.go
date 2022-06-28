package firm

import (
	"context"

	"github.com/tokenized/pkg/logger"
	spynode "github.com/tokenized/spynode/pkg/client"
)

func (f *Firm) HandleTx(ctx context.Context, spyNodeTx *spynode.Tx) {
	txid := *spyNodeTx.Tx.TxHash()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", txid))

	transaction, err := f.addTx(ctx, txid, spyNodeTx)
	if err != nil {
		logger.Error(ctx, "Failed to add tx : %s", err)
		return
	}
	defer f.transactions.Release(ctx, txid)

	if err := f.updateTransaction(ctx, transaction); err != nil {
		logger.Error(ctx, "Failed to update tx : %s", err)
		return
	}
}

func (f *Firm) HandleTxUpdate(ctx context.Context, txUpdate *spynode.TxUpdate) {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", txUpdate.TxID))

	transaction, err := f.transactions.Get(ctx, txUpdate.TxID)
	if err != nil {
		logger.Error(ctx, "Failed to get tx : %s", err)
		return
	}

	if transaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txUpdate.TxID),
		}, "Update transaction not found")
		return
	}
	defer f.transactions.Release(ctx, txUpdate.TxID)

	if txUpdate.State.MerkleProof != nil {
		mp := txUpdate.State.MerkleProof.ConvertToMerkleProof(txUpdate.TxID)
		transaction.Lock()
		if transaction.AddMerkleProof(mp) {
			transaction.Unlock()
			if err := f.transactions.Save(ctx, transaction); err != nil {
				logger.Error(ctx, "Failed to save tx : %s", err)
				return
			}
		}
		transaction.Unlock()
	}

	if err := f.updateTransaction(ctx, transaction); err != nil {
		logger.Error(ctx, "Failed to update tx : %s", err)
		return
	}
}

func (f *Firm) HandleHeaders(ctx context.Context, headers *spynode.Headers) {

}

func (f *Firm) HandleInSync(ctx context.Context) {

}

func (f *Firm) HandleMessage(ctx context.Context, payload spynode.MessagePayload) {
	switch msg := payload.(type) {
	case *spynode.AcceptRegister:
		logger.Info(ctx, "Spynode registration accepted")

		if err := f.spyNodeClient.Ready(ctx, msg.MessageCount); err != nil {
			logger.Error(ctx, "Failed to notify spynode ready : %s", err)
		}

		// if s.nextSpyNodeMessageID == 0 || s.nextSpyNodeMessageID > msg.MessageCount {
		// 	logger.WarnWithFields(ctx, []logger.Field{
		// 		logger.Uint64("next_message_id", s.nextSpyNodeMessageID),
		// 		logger.Uint64("message_count", msg.MessageCount),
		// 	}, "Resetting next message id")
		// 	s.nextSpyNodeMessageID = 1 // first message is 1
		// }

		// if err := f.spyNodeClient.Ready(ctx, s.nextSpyNodeMessageID); err != nil {
		// 	logger.Error(ctx, "Failed to notify spynode ready : %s", err)
		// }

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("next_message_id", msg.MessageCount+1),
		}, "Spynode client ready")
	}
}
