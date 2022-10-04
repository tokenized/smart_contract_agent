package agents

import (
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

// addResponseInput adds an input to the tx that spends the specified output of the inputTx, unless
// it was already added.
func addResponseInput(tx *txbuilder.TxBuilder, inputTx *wire.MsgTx, index int) error {
	inputTxHash := inputTx.TxHash()
	for _, txin := range tx.MsgTx.TxIn {
		if txin.PreviousOutPoint.Hash.Equal(inputTxHash) &&
			txin.PreviousOutPoint.Index == uint32(index) {
			return nil // already have this input
		}
	}

	outpoint := wire.OutPoint{
		Hash:  *inputTxHash,
		Index: uint32(index),
	}
	output := inputTx.TxOut[index]
	if err := tx.AddInput(outpoint, output.LockingScript, output.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	return nil
}

// addDustLockingScript returns the index of an output with the specified locking script or adds a
// dust output if it doesn't exist.
func addDustLockingScript(tx *txbuilder.TxBuilder, lockingScript bitcoin.Script) (uint32, error) {
	for index, txout := range tx.MsgTx.TxOut {
		if txout.LockingScript.Equal(lockingScript) {
			return uint32(index), nil
		}
	}

	index := uint32(len(tx.MsgTx.TxOut))
	if err := tx.AddOutput(lockingScript, 1, false, true); err != nil {
		return 0, errors.Wrap(err, "add output")
	}

	return index, nil
}

func appendLockingScript(lockingScripts []bitcoin.Script,
	lockingScript bitcoin.Script) []bitcoin.Script {
	for _, ls := range lockingScripts {
		if ls.Equal(lockingScript) {
			return lockingScripts
		}
	}

	return append(lockingScripts, lockingScript)
}
