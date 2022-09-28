package agents

import (
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
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

func revertPendingBalances(balances []*state.Balance) {
	for _, balance := range balances {
		balance.RevertPending()
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
