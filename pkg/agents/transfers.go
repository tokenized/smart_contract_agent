package agents

import (
	"context"
	"fmt"

	channels_agent "github.com/tokenized/channels/smart_contract_agent"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction TransactionWithOutputs,
	transfer *actions.Transfer) error {

	// Verify appropriate output belongs to this contract.
	found := false
	agentLockingScript := a.LockingScript()
	for _, instrument := range transfer.Instruments {
		if int(instrument.ContractIndex) >= transaction.OutputCount() {
			logger.Error(ctx, "Invalid transfer contract index: %d >= %d", instrument.ContractIndex,
				transaction.OutputCount())
			return nil
		}

		contractOutput := transaction.Output(int(instrument.ContractIndex))
		if agentLockingScript.Equal(contractOutput.LockingScript) {
			found = true
		}

		fields := []logger.Field{
			logger.Stringer("locking_script", agentLockingScript),
		}

		instrumentID, err := protocol.InstrumentIDForTransfer(instrument)
		if err == nil {
			fields = append(fields, logger.String("instrument_id", instrumentID))
		}

		logger.InfoWithFields(ctx, fields, "Processing transfer")
	}

	if !found {
		return nil // Not for this contract
	}

	return nil
}

func (a *Agent) getSettlementSubscriptions(ctx context.Context, transaction TransactionWithOutputs,
	settlement *actions.Settlement) (state.Subscriptions, error) {

	outputCount := transaction.OutputCount()
	var lockingScripts []bitcoin.Script
	for _, instrument := range settlement.Instruments {
		for _, settle := range instrument.Settlements {
			if int(settle.Index) >= outputCount {
				return nil, fmt.Errorf("Invalid settlement output index: %d >= %d", settle.Index,
					outputCount)
			}

			lockingScript := transaction.Output(int(settle.Index)).LockingScript
			lockingScripts = append(lockingScripts, lockingScript)
		}
	}

	return a.subscriptions.GetLockingScriptMulti(ctx, a.LockingScript(), lockingScripts)
}

func (a *Agent) processSettlement(ctx context.Context, transaction TransactionWithOutputs,
	settlement *actions.Settlement) error {
	txid := transaction.TxID()

	agentLockingScript := a.LockingScript()

	outputCount := transaction.OutputCount()
	var lockingScripts []bitcoin.Script

	// Update one instrument at a time.
	for _, instrument := range settlement.Instruments {
		if int(instrument.ContractIndex) >= transaction.InputCount() {
			logger.Error(ctx, "Invalid settlement contract index: %d >= %d",
				instrument.ContractIndex, transaction.InputCount())
			return nil
		}

		contractLockingScript, err := transaction.InputLockingScript(int(instrument.ContractIndex))
		if err != nil {
			return errors.Wrap(err, "input locking script")
		}

		if !contractLockingScript.Equal(agentLockingScript) {
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

		fields := []logger.Field{
			logger.Stringer("locking_script", agentLockingScript),
		}

		instrumentID, err := protocol.InstrumentIDForSettlement(instrument)
		if err == nil {
			fields = append(fields, logger.String("instrument_id", instrumentID))
		}

		logger.InfoWithFields(ctx, fields, "Processing settlement")

		// Build balances based on the instrument's settlement quantities.
		balances := make([]*state.Balance, len(instrument.Settlements))
		for i, settle := range instrument.Settlements {
			if int(settle.Index) >= outputCount {
				logger.Error(ctx, "Invalid settlement output index: %d >= %d", settle.Index,
					outputCount)
				return nil
			}

			lockingScript := transaction.Output(int(settle.Index)).LockingScript
			lockingScripts = append(lockingScripts, lockingScript)

			balances[i] = &state.Balance{
				LockingScript: lockingScript,
				Quantity:      settle.Quantity,
				Timestamp:     settlement.Timestamp,
				TxID:          &txid,
			}
		}

		// Add the balances to the cache.
		addedBalances, err := a.balances.AddMulti(ctx, agentLockingScript, instrumentCode,
			balances)
		if err != nil {
			return errors.Wrap(err, "add balances")
		}

		// Update any balances that weren't new and therefore weren't updated by the "add".
		for i, balance := range balances {
			if balance == addedBalances[i] {
				continue // balance was new and already up to date from the add.
			}

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
			addedBalances[i].MarkModified()
			addedBalances[i].Unlock()
		}

		a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, addedBalances)
	}

	subscriptions, err := a.subscriptions.GetLockingScriptMulti(ctx, agentLockingScript,
		lockingScripts)
	if err != nil {
		logger.Error(ctx, "Failed to get locking script subscriptions : %s", err)
		return nil
	}
	defer a.subscriptions.ReleaseMulti(ctx, agentLockingScript, subscriptions)

	if len(subscriptions) == 0 {
		return nil
	}

	expandedTx, err := a.transactions.GetExpandedTx(ctx, txid)
	if err != nil {
		logger.Error(ctx, "Failed to get expanded tx : %s", err)
		return nil
	}
	if expandedTx == nil {
		logger.Error(ctx, "Expanded tx not found")
		return nil
	}
	defer a.transactions.Release(ctx, txid)

	msg := &channels_agent.Action{
		Tx: expandedTx,
	}

	for _, subscription := range subscriptions {
		if subscription == nil {
			continue
		}

		subscription.Lock()
		channelHash := subscription.GetChannelHash()
		subscription.Unlock()

		// Send settlement over channel
		channel, err := a.GetChannel(ctx, channelHash)
		if err != nil {
			logger.Error(ctx, "Failed to get subscription channel : %s", err)
			return nil
		}
		if channel == nil {
			continue
		}

		if err := channel.SendMessage(ctx, msg); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("channel", channelHash),
			}, "Failed to send channels message : %s", err)
		}
	}

	return nil
}
