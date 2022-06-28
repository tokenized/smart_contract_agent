package agents

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction TransactionWithOutputs,
	index int, transfer *actions.Transfer) error {

	logger.Info(ctx, "Processing transfer")

	return nil
}

func (a *Agent) processSettlement(ctx context.Context, transaction TransactionWithOutputs,
	index int, settlement *actions.Settlement) error {
	txid := transaction.TxID()

	logger.Info(ctx, "Processing settlement")

	contractLockingScript := a.LockingScript()

	// Update one instrument at a time.
	for _, instrument := range settlement.Instruments {
		contractLS, err := transaction.InputLockingScript(int(instrument.ContractIndex))
		if err != nil {
			logger.Error(ctx, "Invalid settlement contract index: %d: %s", instrument.ContractIndex,
				err)
			return nil
		}

		if !contractLS.Equal(contractLockingScript) {
			continue
		}

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrument.InstrumentCode)

		a.contract.Lock()
		stateInstrument := a.contract.GetInstrument(instrumentCode)
		a.contract.Unlock()

		if stateInstrument == nil {
			logger.Error(ctx, "Instrument not found: %s", instrumentCode)
			return nil
		}

		// Build balances based on the instrument's settlement quantities.
		outputCount := transaction.OutputCount()
		balances := make([]*state.Balance, len(instrument.Settlements))
		for i, settle := range instrument.Settlements {
			if int(settle.Index) >= outputCount {
				logger.Error(ctx, "Invalid settlement output index: %d: %s", settle.Index, err)
				return nil
			}

			balances[i] = &state.Balance{
				LockingScript: transaction.Output(int(settle.Index)).LockingScript,
				Quantity:      settle.Quantity,
				Timestamp:     settlement.Timestamp,
				TxID:          &txid,
			}
		}

		// Add the balances to the cache.
		addedBalances, err := a.balances.AddMulti(ctx, contractLockingScript, instrumentCode,
			balances)
		if err != nil {
			return errors.Wrap(err, "get balances")
		}
		defer a.balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, addedBalances)

		// Update any balances that weren't new and therefore weren't updated by the "add".
		for i, balance := range balances {
			if balance != addedBalances[i] {
				// If the balance doesn't match then it already existed and must be updated with a
				// manual merge and save.
				addedBalances[i].Lock()
				if settlement.Timestamp < addedBalances[i].Timestamp {
					logger.WarnWithFields(ctx, []logger.Field{
						logger.Timestamp("timestamp", int64(addedBalances[i].Timestamp)),
						logger.Timestamp("existing_timestamp", int64(settlement.Timestamp)),
						logger.Stringer("locking_script", balance.LockingScript),
					}, "Older settlement")
					addedBalances[i].Unlock()
					continue
				}

				// Update balance
				addedBalances[i].Quantity = balance.Quantity
				addedBalances[i].Timestamp = settlement.Timestamp
				addedBalances[i].TxID = &txid
				addedBalances[i].Unlock()

				if err := a.balances.Save(ctx, contractLockingScript, instrumentCode,
					addedBalances[i]); err != nil {
					return errors.Wrap(err, "save balance")
				}
			}
		}
	}

	return nil
}

func appendIfDoesntExist(lockingScripts []bitcoin.Script,
	lockingScript bitcoin.Script) []bitcoin.Script {

	for _, script := range lockingScripts {
		if script.Equal(lockingScript) {
			return lockingScripts
		}
	}

	return append(lockingScripts, lockingScript)
}