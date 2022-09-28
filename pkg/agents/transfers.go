package agents

import (
	"context"
	"fmt"

	channels_expanded_tx "github.com/tokenized/channels/expanded_tx"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction *state.Transaction,
	transfer *actions.Transfer) error {

	// Verify appropriate output belongs to this contract.
	isRelevant := false
	agentLockingScript := a.LockingScript()

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("contract_locking_script", agentLockingScript))

	// TODO Verify there is not more than one transfer per instrument. --ce

	transaction.Lock()
	defer transaction.Unlock()

	txid := transaction.TxID()

	for _, instrumentTransfer := range transfer.Instruments {
		if int(instrumentTransfer.ContractIndex) >= transaction.OutputCount() {
			logger.Warn(ctx, "Invalid transfer contract index: %d >= %d",
				instrumentTransfer.ContractIndex, transaction.OutputCount())
			return nil
		}

		contractOutput := transaction.Output(int(instrumentTransfer.ContractIndex))
		if !agentLockingScript.Equal(contractOutput.LockingScript) {
			continue
		}

		isRelevant = true

		instrumentCtx := ctx
		instrumentID, err := protocol.InstrumentIDForTransfer(instrumentTransfer)
		if err == nil {
			instrumentCtx = logger.ContextWithLogFields(ctx,
				logger.String("instrument_id", instrumentID))
		}

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		a.contract.Lock()
		stateInstrument := a.contract.GetInstrument(instrumentCode)
		a.contract.Unlock()

		if stateInstrument == nil {
			logger.Warn(instrumentCtx, "Instrument not found: %s", instrumentCode)
			return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
				actions.RejectionsInstrumentNotFound, "")
		}

		if instrumentTransfer.InstrumentType != string(stateInstrument.InstrumentType[:]) {
			logger.Warn(instrumentCtx, "Wrong instrument type: %s (should be %s)",
				instrumentTransfer.InstrumentType, stateInstrument.InstrumentType)
			return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
				actions.RejectionsInstrumentNotFound, "wrong instrument type")
		}

		logger.Info(ctx, "Processing transfer")

		// TODO Check instrument's transfer rules. --ce
		// now := uint64(time.Now().UnixNano())

		// Get relevant balances.
		// TODO Locking scripts might need to be in order or something so when two different actions
		// are running and getting mutexes they will not get deadlocks by having a locking script
		// locked by another transfer while the other transfer already has a lock on a locking
		// script needed by this transfer. --ce
		var addBalances []*state.Balance
		var senderLockingScripts []bitcoin.Script
		for _, sender := range instrumentTransfer.InstrumentSenders {
			inputOutput, err := transaction.InputOutput(int(sender.Index))
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid sender index : %s", err)
				return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
					actions.RejectionsMsgMalformed, "invalid sender index")
			}

			senderLockingScripts = append(senderLockingScripts, inputOutput.LockingScript)
			addBalances = appendZeroBalance(addBalances, inputOutput.LockingScript)
		}

		var receiverLockingScripts []bitcoin.Script
		for _, receiver := range instrumentTransfer.InstrumentReceivers {
			ra, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address : %s", err)
				return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
					actions.RejectionsMsgMalformed, "invalid receiver address")
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address script : %s", err)
				return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
					actions.RejectionsMsgMalformed, "invalid receiver address script")
			}

			receiverLockingScripts = append(receiverLockingScripts, lockingScript)
			addBalances = appendZeroBalance(addBalances, lockingScript)
		}

		balances, err := a.balances.AddMulti(instrumentCtx, agentLockingScript, instrumentCode,
			addBalances)
		if err != nil {
			return errors.Wrap(err, "add balances")
		}
		defer a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

		lockBalances(balances)
		// TODO If there is more than one lock on the same balance then defer won't work. There
		// shouldn't be if there is only one transfer per instrument. --ce
		defer unlockBalances(balances)
		defer revertPendingBalances(balances)

		for i, sender := range instrumentTransfer.InstrumentSenders {
			lockingScript := senderLockingScripts[i]
			balance := findBalance(balances, lockingScript)
			if balance == nil {
				return fmt.Errorf("Missing balance for sender %d : %s", i, lockingScript)
			}

			if err := balance.AddPendingDebit(sender.Quantity); err != nil {
				return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
					actions.RejectionsInsufficientQuantity, fmt.Sprintf("sender %d: %s", i, err))
			}
		}

		for i, receiver := range instrumentTransfer.InstrumentReceivers {
			lockingScript := receiverLockingScripts[i]
			balance := findBalance(balances, lockingScript)
			if balance == nil {
				return fmt.Errorf("Missing balance for receiver %d : %s", i, lockingScript)
			}

			if err := balance.AddPendingCredit(receiver.Quantity); err != nil {
				return a.sendReject(ctx, transaction, int(instrumentTransfer.ContractIndex),
					actions.RejectionsInsufficientQuantity, err.Error())
			}
		}

		finalizePendingBalances(balances, &txid)

		// TODO Revert balance changes if any part of transfer fails after starting to update
		// balances. --ce
	}

	if !isRelevant {
		return nil // Not for this contract
	}

	return nil
}

func (a *Agent) getSettlementSubscriptions(ctx context.Context, transaction *state.Transaction,
	settlement *actions.Settlement) (state.Subscriptions, error) {

	transaction.Lock()
	outputCount := transaction.OutputCount()
	var lockingScripts []bitcoin.Script
	for _, instrument := range settlement.Instruments {
		for _, settle := range instrument.Settlements {
			if int(settle.Index) >= outputCount {
				transaction.Unlock()
				return nil, fmt.Errorf("Invalid settlement output index: %d >= %d", settle.Index,
					outputCount)
			}

			lockingScript := transaction.Output(int(settle.Index)).LockingScript
			lockingScripts = append(lockingScripts, lockingScript)
		}
	}
	transaction.Unlock()

	return a.subscriptions.GetLockingScriptMulti(ctx, a.LockingScript(), lockingScripts)
}

func (a *Agent) processSettlement(ctx context.Context, transaction *state.Transaction,
	settlement *actions.Settlement) error {
	transaction.Lock()
	defer transaction.Unlock()
	txid := transaction.TxID()

	agentLockingScript := a.LockingScript()

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("contract_locking_script", agentLockingScript))

	outputCount := transaction.OutputCount()
	var lockingScripts []bitcoin.Script

	// Update one instrument at a time.
	for _, instrument := range settlement.Instruments {
		if int(instrument.ContractIndex) >= transaction.InputCount() {
			logger.Error(ctx, "Invalid settlement contract index: %d >= %d",
				instrument.ContractIndex, transaction.InputCount())
			return nil
		}

		contractInputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
		if err != nil {
			return errors.Wrap(err, "contract input locking script")
		}

		if !contractInputOutput.LockingScript.Equal(agentLockingScript) {
			continue
		}

		instrumentCtx := ctx

		instrumentID, err := protocol.InstrumentIDForSettlement(instrument)
		if err == nil {
			instrumentCtx = logger.ContextWithLogFields(instrumentCtx,
				logger.String("instrument_id", instrumentID))
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

		logger.Info(instrumentCtx, "Processing settlement")

		// Build balances based on the instrument's settlement quantities.
		balances := make([]*state.Balance, len(instrument.Settlements))
		for i, settle := range instrument.Settlements {
			if int(settle.Index) >= outputCount {
				logger.ErrorWithFields(instrumentCtx, []logger.Field{
					logger.Int("settlement_index", i),
					logger.Uint32("output_index", settle.Index),
					logger.Int("output_count", outputCount),
				}, "Invalid settlement output index")
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
		addedBalances, err := a.balances.AddMulti(instrumentCtx, agentLockingScript, instrumentCode,
			balances)
		if err != nil {
			return errors.Wrap(err, "add balances")
		}

		// Update any balances that weren't new and therefore weren't updated by the "add".
		for i, balance := range balances {
			if balance == addedBalances[i] {
				continue // balance was new and already up to date from the add.
			}

			// If the balance doesn't match then it already existed and must be updated.
			addedBalances[i].Lock()
			if settlement.Timestamp < addedBalances[i].Timestamp {
				logger.WarnWithFields(instrumentCtx, []logger.Field{
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

		for _, balance := range addedBalances {
			logger.InfoWithFields(instrumentCtx, []logger.Field{
				logger.Stringer("locking_script", balance.LockingScript),
				logger.Uint64("quantity", balance.Quantity),
			}, "Balance settled")
		}

		a.balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode, addedBalances)
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

	expandedTx, err := transaction.ExpandedTx(ctx)
	if err != nil {
		logger.Error(ctx, "Failed to get expanded tx : %s", err)
		return nil
	}
	if expandedTx == nil {
		logger.Error(ctx, "Expanded tx not found")
		return nil
	}

	msg := channels_expanded_tx.ExpandedTxMessage(*expandedTx)

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

		if err := channel.SendMessage(ctx, &msg); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("channel", channelHash),
			}, "Failed to send channels message : %s", err)
		}
	}

	return nil
}
