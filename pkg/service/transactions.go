package service

import (
	"context"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/smart_contract_agent/internal/state"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

func (s *Service) addTx(ctx context.Context, txid bitcoin.Hash32,
	spyNodeTx *spynode.Tx) (*state.Transaction, error) {

	transaction := &state.Transaction{
		Tx: spyNodeTx.Tx,
	}

	transaction.SpentOutputs = make([]*expanded_tx.Output, len(spyNodeTx.Outputs))
	for i, txout := range spyNodeTx.Outputs {
		transaction.SpentOutputs[i] = &expanded_tx.Output{
			Value:         txout.Value,
			LockingScript: txout.LockingScript,
		}
	}

	if spyNodeTx.State.Safe {
		transaction.State = wallet.TxStateSafe
	} else {
		if spyNodeTx.State.UnSafe {
			transaction.State |= wallet.TxStateUnsafe
		}
		if spyNodeTx.State.Cancelled {
			transaction.State |= wallet.TxStateCancelled
		}
	}

	if spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		transaction.MerkleProofs = []*merkle_proof.MerkleProof{mp}
	}

	addedTx, err := s.caches.Transactions.Add(ctx, transaction)
	if err != nil {
		return nil, errors.Wrap(err, "add tx")
	}

	if addedTx != transaction && spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		// Transaction already existed, so try to add the merkle proof to it.
		addedTx.Lock()
		addedTx.AddMerkleProof(mp)
		addedTx.Unlock()
	}

	return addedTx, nil
}
