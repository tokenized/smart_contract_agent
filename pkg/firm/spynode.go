package firm

import (
	"context"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/logger"
	spynode "github.com/tokenized/spynode/pkg/client"
)

func (f *Firm) HandleTx(ctx context.Context, spyNodeTx *spynode.Tx) {
	txid := *spyNodeTx.Tx.TxHash()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", txid))

	f.UpdateNextSpyNodeMessageID(spyNodeTx.ID)

	transaction, err := f.addTx(ctx, txid, spyNodeTx)
	if err != nil {
		logger.Error(ctx, "Failed to add tx : %s", err)
		return
	}
	defer f.transactions.Release(ctx, txid)

	if err := f.UpdateTransaction(ctx, transaction); err != nil {
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

	modified := false
	transaction.Lock()
	if txUpdate.State.Safe {
		if transaction.State&wallet.TxStateSafe == 0 {
			transaction.State = wallet.TxStateSafe
			modified = true
		}
	} else {
		if txUpdate.State.UnSafe {
			if transaction.State&wallet.TxStateUnsafe == 0 {
				transaction.State |= wallet.TxStateUnsafe
				modified = true
			}
		}
		if txUpdate.State.Cancelled {
			if transaction.State&wallet.TxStateCancelled == 0 {
				transaction.State |= wallet.TxStateCancelled
				modified = true
			}
		}
	}

	if txUpdate.State.MerkleProof != nil {
		mp := txUpdate.State.MerkleProof.ConvertToMerkleProof(txUpdate.TxID)
		if transaction.AddMerkleProof(mp) {
			modified = true
		}
	}

	transaction.Unlock()

	if modified {
		if err := f.transactions.Save(ctx, transaction); err != nil {
			logger.Error(ctx, "Failed to save tx : %s", err)
			return
		}
	}

	if err := f.UpdateTransaction(ctx, transaction); err != nil {
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

		nextMessageID := f.NextSpyNodeMessageID()
		if err := f.spyNodeClient.Ready(ctx, nextMessageID); err != nil {
			logger.Error(ctx, "Failed to notify spynode ready : %s", err)
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("next_message", nextMessageID),
			logger.Uint64("message_count", msg.MessageCount),
		}, "Spynode client ready")
	}
}
