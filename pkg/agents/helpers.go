package agents

import (
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/pkg/errors"
)

// appendLockingScript appends a locking script if it doesn't already exist.
func appendLockingScript(lockingScripts []bitcoin.Script,
	lockingScript bitcoin.Script) []bitcoin.Script {

	for _, ls := range lockingScripts {
		if ls.Equal(lockingScript) {
			return lockingScripts // already exists
		}
	}

	return append(lockingScripts, lockingScript)
}

func appendZeroBalance(balances []*state.Balance, lockingScript bitcoin.Script) []*state.Balance {
	for _, balance := range balances {
		if balance.LockingScript.Equal(lockingScript) {
			return balances // already contains so no append
		}
	}

	return append(balances, &state.Balance{
		LockingScript: lockingScript,
	})
}

func findBalance(balances []*state.Balance, lockingScript bitcoin.Script) *state.Balance {
	for _, balance := range balances {
		if balance.LockingScript.Equal(lockingScript) {
			return balance
		}
	}

	return nil
}

func revertPendingBalances(balances []*state.Balance, txid *bitcoin.Hash32) {
	for _, balance := range balances {
		balance.RevertPending(txid)
	}
}

func finalizePendingBalances(balances []*state.Balance, txid *bitcoin.Hash32) {
	for _, balance := range balances {
		balance.FinalizePending(txid)
	}
}

func lockBalances(balances []*state.Balance) {
	for _, balance := range balances {
		balance.Lock()
	}
}

func unlockBalances(balances []*state.Balance) {
	for _, balance := range balances {
		balance.Unlock()
	}
}

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
