package agents

import (
	"bytes"
	"context"
	"fmt"

	channels_expanded_tx "github.com/tokenized/channels/expanded_tx"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction *state.Transaction,
	transfer *actions.Transfer, now uint64) error {

	// Verify appropriate output belongs to this contract.
	agentLockingScript := a.LockingScript()

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("contract_locking_script", agentLockingScript))

	txid := transaction.GetTxID()
	transferContracts, err := parseTransferContracts(transaction, transfer, agentLockingScript, now)
	if err != nil {
		if errors.Cause(err) == ErrNotRelevant {
			logger.Warn(ctx, "Transfer not relevant to this contract agent")
			return nil // Not for this contract
		}

		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "parse contracts")
	}

	if !transferContracts.IsFirstContract() {
		logger.Info(ctx, "Waiting for settlement request message to process transfer")
		return nil // Wait for settlement request message
	}

	firstContractIndex := transferContracts.OutputIndexes[0]

	// Check if contract is frozen.
	if a.ContractIsExpired(now) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Contract is expired")
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectErrorWithOutputIndex(actions.RejectionsContractExpired, "", now,
				firstContractIndex)), "reject")
	}

	// Verify expiry.
	if transfer.OfferExpiry != 0 && now > transfer.OfferExpiry {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("expiry", int64(transfer.OfferExpiry)),
			logger.Timestamp("now", int64(now)),
		}, "Transfer offer expired")
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectErrorWithOutputIndex(actions.RejectionsTransferExpired, "", now,
				firstContractIndex)), "reject")
	}

	// Verify there are no duplicate instruments.
	var instrumentCodes [][]byte
	for _, instrumentTransfer := range transfer.Instruments {
		if len(instrumentTransfer.InstrumentSenders) == 0 {
			logger.Warn(ctx, "Transfer is missing senders")
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, "missing senders",
					now, firstContractIndex)), "reject")
		}

		if len(instrumentTransfer.InstrumentReceivers) == 0 {
			logger.Warn(ctx, "Transfer is missing receivers")
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, "missing receivers",
					now, firstContractIndex)), "reject")
		}

		instrumentCode := instrumentTransfer.InstrumentCode
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			instrumentCode = nil
		}

		for _, instrument := range instrumentCodes {
			if bytes.Equal(instrument, instrumentCode) {
				instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
				logger.WarnWithFields(ctx, []logger.Field{
					logger.String("instrument_id", instrumentID),
				}, "Duplicate instrument in transfer")
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
						"duplicate instrument", now, firstContractIndex)), "reject")
			}
		}

		instrumentCodes = append(instrumentCodes, instrumentCode)
	}

	// Process transfers.
	settlementTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())
	settlement := &actions.Settlement{
		Timestamp: now,
	}

	var balances state.Balances
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			if len(instrumentTransfer.InstrumentCode) != 0 {
				logger.Warn(ctx, "Bitcoin instrument with instrument code")
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
						"bitcoin transfer with instrument code", now, firstContractIndex)),
					"reject")
			}

			// TODO Handle bitcoin transfers. --ce
			logger.Info(ctx, "Bitcoin transfer")
			continue
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		instrumentCtx := ctx
		instrumentID, err := protocol.InstrumentIDForTransfer(instrumentTransfer)
		if err == nil {
			instrumentCtx = logger.ContextWithLogFields(ctx,
				logger.String("instrument_id", instrumentID))
		}
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		instrumentSettlement, instrumentBalances, err := a.buildInstrumentSettlement(instrumentCtx,
			settlementTx, settlement, transaction, instrumentCode, instrumentTransfer,
			transferContracts.Outputs[index], transferContracts.IsMultiContract(), now)
		if err != nil {
			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				return errors.Wrap(a.sendRejection(instrumentCtx, transaction, rejectError),
					"reject")
			}
			return errors.Wrapf(err, "build settlement: %s", instrumentID)
		}

		settlement.Instruments = append(settlement.Instruments, instrumentSettlement)
		defer a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, instrumentBalances)
		balances = state.AppendBalances(balances, instrumentBalances)
	}

	settlementScript, err := protocol.Serialize(settlement, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize settlement")
	}

	if err := settlementTx.AddOutput(settlementScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add settlement output")
	}

	// Add the exchange fee
	if transfer.ExchangeFee > 0 {
		ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
		if err != nil {
			logger.Warn(ctx, "Invalid exchange fee address : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(), now,
					firstContractIndex)), "reject")
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(), now,
					firstContractIndex)), "reject")
		}

		if err := settlementTx.AddOutput(lockingScript, transfer.ExchangeFee, false,
			false); err != nil {
			return errors.Wrap(err, "add exchange fee")
		}
	}

	// Add the contract fee
	if a.ContractFee() > 0 {
		if err := settlementTx.AddOutput(a.FeeLockingScript(), a.ContractFee(), true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := settlementTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	if transferContracts.IsMultiContract() {
		balances.Unlock()

		// Send a settlement request to the next contract.
		if err := a.sendSettlementRequest(ctx, transaction, transaction, transfer,
			transferContracts, settlement, now); err != nil {
			return errors.Wrap(err, "send settlement request")
		}

		return nil
	}

	// Sign settlement tx.
	if _, err := settlementTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectErrorWithOutputIndex(actions.RejectionsInsufficientTxFeeFunding,
					err.Error(), now, firstContractIndex)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	if err := a.completeSettlement(ctx, txid, settlementTx, balances, now); err != nil {
		return errors.Wrap(err, "complete settlement")
	}

	balances.Unlock()
	return nil
}

func (a *Agent) completeSettlement(ctx context.Context, transferTxID bitcoin.Hash32,
	settlementTx *txbuilder.TxBuilder, balances state.Balances, now uint64) error {

	settlementTxID := *settlementTx.MsgTx.TxHash()
	settlementTransaction, err := a.transactions.AddRaw(ctx, settlementTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, settlementTxID)

	// Settle balances regardless of tx acceptance by the network as the agent is the single source
	// of truth.
	balances.Settle(transferTxID, settlementTxID, now)

	// Set settlement tx as processed since all the balances were just settled.
	settlementTransaction.Lock()
	settlementTransaction.SetProcessed()
	settlementTransaction.Unlock()

	// Broadcast settlement tx.
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", settlementTxID),
	}, "Responding with settlement")
	if err := a.BroadcastTx(ctx, settlementTx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postSettlementToSubscriptions(ctx, balances.LockingScripts(),
		settlementTransaction); err != nil {
		return errors.Wrap(err, "post settlement")
	}

	return nil
}

func (a *Agent) processSettlement(ctx context.Context, transaction *state.Transaction,
	settlement *actions.Settlement, now uint64) error {
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
		balances := make(state.Balances, len(instrument.Settlements))
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

	if err := a.postSettlementToSubscriptions(ctx, lockingScripts, transaction); err != nil {
		return errors.Wrap(err, "post settlement")
	}

	return nil
}

// postSettlementToSubscriptions posts the settlment transaction to any subscriptsions for the
// relevant locking scripts.
func (a *Agent) postSettlementToSubscriptions(ctx context.Context, lockingScripts []bitcoin.Script,
	transaction *state.Transaction) error {

	agentLockingScript := a.LockingScript()

	subscriptions, err := a.subscriptions.GetLockingScriptMulti(ctx, agentLockingScript,
		lockingScripts)
	if err != nil {
		return errors.Wrap(err, "get subscriptions")
	}
	defer a.subscriptions.ReleaseMulti(ctx, agentLockingScript, subscriptions)

	if len(subscriptions) == 0 {
		return nil
	}

	expandedTx, err := transaction.ExpandedTx(ctx)
	if err != nil {
		return errors.Wrap(err, "get expanded tx")
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
			return errors.Wrapf(err, "get channel : %s", channelHash)
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

// populateTransferSettlement adds all the new balances to the transfer settlement.
func populateTransferSettlement(tx *txbuilder.TxBuilder,
	transferSettlement *actions.InstrumentSettlementField, balances state.Balances) error {

	for i, balance := range balances {
		index, err := addDustLockingScript(tx, balance.LockingScript)
		if err != nil {
			return errors.Wrapf(err, "add locking script %d", i)
		}

		transferSettlement.Settlements = append(transferSettlement.Settlements,
			&actions.QuantityIndexField{
				Quantity: balance.SettlePendingQuantity(),
				Index:    index,
			})
	}

	return nil
}

type TransferContracts struct {
	// LockingScripts is the list of contract agent locking scripts in order they should be
	// processed.
	LockingScripts []bitcoin.Script
	CurrentIndex   int // Index of LockingScripts that matches current contract agent

	PreviousLockingScript bitcoin.Script // Contract agent locking script before current agent.
	NextLockingScript     bitcoin.Script // Contract agent locking script after current agent.

	// Outputs are the transfer tx outputs that correspond to each of the instrument transfers.
	Outputs       []*wire.TxOut
	OutputIndexes []int // Index in the transfer tx of each output for each instrument transfer.

	// BoomerangOutput is the output used by the first contract to fund the settlement requests and
	// signature requests.
	BoomerangOutput      *wire.TxOut
	BoomerangOutputIndex int
}

// IsPriorContract returns true if the specified locking script is for a contract that is processed
// before the current contract.
func (c TransferContracts) IsPriorContract(lockingScript bitcoin.Script) bool {
	for i, ls := range c.LockingScripts {
		if i == c.CurrentIndex {
			return false
		}

		if ls.Equal(lockingScript) {
			return true
		}
	}

	return false
}

// IsFinalContract returns true if the current contract is the final contract to process this
// transfer.
func (c TransferContracts) IsFinalContract() bool {
	return c.CurrentIndex == len(c.LockingScripts)-1
}

func (c TransferContracts) IsFirstContract() bool {
	return c.CurrentIndex == 0
}

func (c TransferContracts) IsMultiContract() bool {
	return len(c.LockingScripts) > 1
}

// parseContracts returns the previous, next, and full list of contracts in the order that they
// should process the transfer.
func parseTransferContracts(transferTransaction *state.Transaction, transfer *actions.Transfer,
	currentLockingScript bitcoin.Script, now uint64) (*TransferContracts, error) {

	count := len(transfer.Instruments)
	if count == 0 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"transfer has no instruments", now)
	}

	result := &TransferContracts{
		CurrentIndex:  -1,
		Outputs:       make([]*wire.TxOut, count),
		OutputIndexes: make([]int, count),
	}

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	outputCount := transferTransaction.OutputCount()
	for i, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"transfer tx invalid contract index", now)
		}

		contractOutput := transferTransaction.Output(int(instrumentTransfer.ContractIndex))
		result.Outputs[i] = contractOutput
		result.OutputIndexes[i] = int(instrumentTransfer.ContractIndex)

		isCurrentContract := currentLockingScript.Equal(contractOutput.LockingScript)

		found := false
		for _, contractLockingScript := range result.LockingScripts {
			if contractLockingScript.Equal(contractOutput.LockingScript) {
				found = true
				break
			}
		}

		if !found {
			if result.CurrentIndex != -1 && // this agent's contract found
				len(result.NextLockingScript) == 0 && // don't have next contract yet
				!currentLockingScript.Equal(contractOutput.LockingScript) { // not this agent's contract
				result.NextLockingScript = contractOutput.LockingScript
			}

			if isCurrentContract {
				result.CurrentIndex = len(result.LockingScripts)
				if result.CurrentIndex != 0 { // this agent isn't the first contract
					result.PreviousLockingScript = result.LockingScripts[result.CurrentIndex-1]
				}
			}
			result.LockingScripts = append(result.LockingScripts, contractOutput.LockingScript)
		}
	}

	if result.CurrentIndex == -1 {
		return nil, ErrNotRelevant
	}

	if len(result.LockingScripts) == 1 {
		return result, nil // No boomerang needed for single contract transfer.
	}

	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)

		if !output.LockingScript.Equal(currentLockingScript) {
			continue
		}

		found := false
		for _, instrumentTransfer := range transfer.Instruments {
			if int(instrumentTransfer.ContractIndex) == i {
				found = true
			}
		}

		if !found {
			result.BoomerangOutput = output
			result.BoomerangOutputIndex = i
			break
		}
	}

	return result, nil
}

// buildInstrumentSettlement updates the settlementTx for a settlement and creates a settlement for
// one instrument.
func (a *Agent) buildInstrumentSettlement(ctx context.Context, settlementTx *txbuilder.TxBuilder,
	settlement *actions.Settlement, transferTransaction *state.Transaction,
	instrumentCode state.InstrumentCode, instrumentTransfer *actions.InstrumentTransferField,
	contractOutput *wire.TxOut, isMultiContract bool,
	now uint64) (*actions.InstrumentSettlementField, state.Balances, error) {

	agentLockingScript := a.LockingScript()
	adminLockingScript := a.AdminLockingScript()

	transferTxID := transferTransaction.GetTxID()
	contractInputIndex, err := addResponseInput(settlementTx, transferTxID, contractOutput,
		int(instrumentTransfer.ContractIndex))
	if err != nil {
		return nil, nil, errors.Wrap(err, "add response input")
	}

	a.contract.Lock()
	instrument := a.contract.GetInstrument(instrumentCode)
	a.contract.Unlock()

	if instrument == nil {
		logger.Warn(ctx, "Instrument not found: %s", instrumentCode)
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound, "",
			now, int(instrumentTransfer.ContractIndex))
	}

	instrument.Lock()
	instrumentType := string(instrument.InstrumentType[:])
	transfersPermitted := instrument.TransfersPermitted()
	isFrozen := instrument.IsFrozen(now)
	isExpired := instrument.IsExpired(now)
	instrument.Unlock()

	if instrumentTransfer.InstrumentType != instrumentType {
		logger.Warn(ctx, "Wrong instrument type: %s (should be %s)",
			instrumentTransfer.InstrumentType, instrument.InstrumentType)
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound, "",
			now, int(instrumentTransfer.ContractIndex))
	}

	logger.Info(ctx, "Processing transfer")

	// Check if instrument is frozen.
	if isFrozen {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is frozen")
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentFrozen, "", now,
			int(instrumentTransfer.ContractIndex))
	}

	// Check if instrument is expired or event is over.
	if isExpired {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is expired")
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted,
			"expired", now, int(instrumentTransfer.ContractIndex))
	}

	// TODO Verify receiver signatures if they are required. --ce

	// Get relevant balances.
	// TODO Locking scripts might need to be in order or something so when two different
	// transfers are running concurrently and getting mutexes they will not get deadlocks by
	// having a locking script locked by another transfer while the other transfer already has a
	// lock on a locking script needed by this transfer. --ce
	// There is also the potential for deadlock where this agent has balances locked for one
	// instrument and another agent has locked balances for an instrument.
	var addBalances state.Balances
	var senderLockingScripts []bitcoin.Script
	var senderQuantity uint64
	onlyFromAdmin := true
	onlyToAdmin := true
	for _, sender := range instrumentTransfer.InstrumentSenders {
		if sender.Quantity == 0 {
			logger.Warn(ctx, "Sender quantity is zero")
			return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"sender quantity is zero", now, int(instrumentTransfer.ContractIndex))
		}

		senderQuantity += sender.Quantity

		inputOutput, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			logger.Warn(ctx, "Invalid sender index : %s", err)
			return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid sender index: %d", sender.Index), now,
				int(instrumentTransfer.ContractIndex))
		}

		if !adminLockingScript.Equal(inputOutput.LockingScript) {
			onlyFromAdmin = false
		}

		senderLockingScripts = append(senderLockingScripts, inputOutput.LockingScript)
		addBalances = state.AppendZeroBalance(addBalances, inputOutput.LockingScript)
	}

	var receiverLockingScripts []bitcoin.Script
	var receiverQuantity uint64
	for _, receiver := range instrumentTransfer.InstrumentReceivers {
		if receiver.Quantity == 0 {
			logger.Warn(ctx, "Receiver quantity is zero")
			return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"receiver quantity is zero", now, int(instrumentTransfer.ContractIndex))
		}

		receiverQuantity += receiver.Quantity

		ra, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address : %s", err)
			return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid receiver address: %s", err), now,
				int(instrumentTransfer.ContractIndex))
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address script : %s", err)
			return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid receiver address script: %s", err), now,
				int(instrumentTransfer.ContractIndex))
		}

		if !adminLockingScript.Equal(lockingScript) {
			onlyToAdmin = false
		}

		receiverLockingScripts = append(receiverLockingScripts, lockingScript)
		addBalances = state.AppendZeroBalance(addBalances, lockingScript)
	}

	if !transfersPermitted && !onlyFromAdmin && !onlyToAdmin {
		logger.Warn(ctx, "Transfers not permitted")
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted, "",
			now, int(instrumentTransfer.ContractIndex))
	}

	if senderQuantity != receiverQuantity {
		logger.Warn(ctx,
			"Sender and receiver quantity do not match : sender %d, receiver %d",
			senderQuantity, receiverQuantity)
		return nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
			"sender quantity != receiver quantity", now, int(instrumentTransfer.ContractIndex))
	}

	balances, err := a.balances.AddMulti(ctx, agentLockingScript, instrumentCode,
		addBalances)
	if err != nil {
		return nil, nil, errors.Wrap(err, "add balances")
	}

	balances.Lock()

	for i, sender := range instrumentTransfer.InstrumentSenders {
		lockingScript := senderLockingScripts[i]
		balance := balances.Find(lockingScript)
		if balance == nil {
			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			return nil, nil, fmt.Errorf("Missing balance for sender %d : %s", i, lockingScript)
		}

		if err := balance.AddPendingDebit(sender.Quantity, now); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", sender.Quantity),
			}, "Failed to add debit : %s", err)

			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.InputIndex = int(sender.Index)
				rejectError.OutputIndex = int(instrumentTransfer.ContractIndex)
				rejectError.Message = fmt.Sprintf("sender %d: %s", i, rejectError.Message)
			}

			return nil, nil, errors.Wrap(err, "add debit")
		}
	}

	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		lockingScript := receiverLockingScripts[i]
		balance := balances.Find(lockingScript)
		if balance == nil {
			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			return nil, nil, fmt.Errorf("Missing balance for receiver %d : %s", i, lockingScript)
		}

		if err := balance.AddPendingCredit(receiver.Quantity, now); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", receiver.Quantity),
			}, "Failed to add credit : %s", err)
			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.OutputIndex = int(instrumentTransfer.ContractIndex)
				rejectError.Message = fmt.Sprintf("receiver %d: %s", i, rejectError.Message)
			}

			return nil, nil, errors.Wrap(err, "add credit")
		}
	}

	instrumentSettlement := &actions.InstrumentSettlementField{
		ContractIndex:  contractInputIndex,
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode[:],
	}

	if err := populateTransferSettlement(settlementTx, instrumentSettlement, balances); err != nil {
		balances.RevertPending(&transferTxID)
		balances.Unlock()
		a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
		return nil, nil, errors.Wrap(err, "populate settlement")
	}

	balances.FinalizePending(&transferTxID, isMultiContract)

	return instrumentSettlement, balances, nil
}
