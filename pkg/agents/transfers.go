package agents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	channels_expanded_tx "github.com/tokenized/channels/expanded_tx"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction *state.Transaction,
	transfer *actions.Transfer, now uint64) error {

	// Verify appropriate output belongs to this contract.
	agentLockingScript := a.LockingScript()

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("contract_locking_script", agentLockingScript))

	settlementTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	transaction.Lock()
	txid := transaction.TxID()

	// Check if this is a single contract settlement or if this agent is for the master contract,
	// meaning this contract acts first and last on a multi-contract settlement.
	isRelevant := false
	isMultiContract := false
	isMasterContract := false
	firstContractIndex := -1
	firstInstrument := true
	outputCount := transaction.OutputCount()
	contractOutputs := make([]*wire.TxOut, len(transfer.Instruments))
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			logger.Warn(ctx, "Invalid transfer contract index: %d >= %d",
				instrumentTransfer.ContractIndex, outputCount)
			transaction.Unlock()
			return nil
		}

		contractOutput := transaction.Output(int(instrumentTransfer.ContractIndex))
		contractOutputs[index] = contractOutput
		if agentLockingScript.Equal(contractOutput.LockingScript) {
			isRelevant = true
			if firstInstrument {
				isMasterContract = true
			}
			if firstContractIndex == -1 {
				firstContractIndex = index
			}
		} else {
			isMultiContract = true
		}
		firstInstrument = false
	}
	transaction.Unlock()

	if !isRelevant {
		logger.Warn(ctx, "Transfer not relevant to this contract agent")
		return nil // Not for this contract
	}

	if !isMasterContract {
		logger.Info(ctx, "Waiting for settlement request message to process transfer")
		return nil // Wait for settlement request message
	}

	// Check if contract is frozen.
	if a.ContractIsExpired(now) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Contract is expired")
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectErrorWithOutputIndex(actions.RejectionsContractExpired, "", now,
				firstContractIndex)), "reject")
	}

	// Verify expiry.
	if transfer.OfferExpiry != 0 && now > transfer.OfferExpiry {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("expiry", int64(transfer.OfferExpiry)),
			logger.Timestamp("now", int64(now)),
		}, "Transfer offer expired")
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectErrorWithOutputIndex(actions.RejectionsTransferExpired, "", now,
				firstContractIndex)), "reject")
	}

	// Verify there are no duplicate instruments.
	var instrumentCodes [][]byte
	for _, instrumentTransfer := range transfer.Instruments {
		if len(instrumentTransfer.InstrumentSenders) == 0 {
			logger.Warn(ctx, "Transfer is missing senders")
			return errors.Wrap(a.sendRejection(ctx, transaction,
				NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, "missing senders",
					now, firstContractIndex)), "reject")
		}

		if len(instrumentTransfer.InstrumentReceivers) == 0 {
			logger.Warn(ctx, "Transfer is missing receivers")
			return errors.Wrap(a.sendRejection(ctx, transaction,
				NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, "missing receivers",
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
					NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
						"duplicate instrument", now, firstContractIndex)), "reject")
			}
		}

		instrumentCodes = append(instrumentCodes, instrumentCode)
	}

	// Process transfers.
	settlement := &actions.Settlement{
		Timestamp: now,
	}

	var balances state.Balances
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			if len(instrumentTransfer.InstrumentCode) != 0 {
				logger.Warn(ctx, "Bitcoin instrument with instrument code")
				return errors.Wrap(a.sendRejection(ctx, transaction,
					NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
						"bitcoin transfer with instrument code", now, firstContractIndex)),
					"reject")
			}

			// TODO Handle bitcoin transfers. --ce
			logger.Info(ctx, "Bitcoin transfer")
			continue
		}

		if !agentLockingScript.Equal(contractOutputs[index].LockingScript) {
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

		instrumentSettlement, instrumentBalances, err := a.buildSettlement(instrumentCtx,
			settlementTx, settlement, transaction, instrumentTransfer, contractOutputs[index],
			int(instrumentTransfer.ContractIndex), isMultiContract, now)
		if err != nil {
			if rejectError, ok := err.(RejectError); ok {
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
				NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(), now,
					firstContractIndex)), "reject")
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(), now,
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

	if isMultiContract {
		// Send a settlement request to the next contract.
		if err := a.sendSettlementRequest(ctx, transaction, transaction, transfer, settlementTx,
			settlement, now); err != nil {
			return errors.Wrap(err, "settlement request")
		}

		return nil
	}

	// Sign settlement tx.
	if _, err := settlementTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				NewRejectErrorWithOutputIndex(actions.RejectionsInsufficientTxFeeFunding,
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

func (a *Agent) sendSettlementRequest(ctx context.Context,
	currentTransaction, transferTransaction *state.Transaction, transfer *actions.Transfer,
	settlementTransaction *txbuilder.TxBuilder, settlement *actions.Settlement, now uint64) error {

	agentLockingScript := a.LockingScript()
	currentTxID := currentTransaction.GetTxID()
	transferTxID := transferTransaction.GetTxID()
	fundingIndex := uint32(0xffffffff)
	if !currentTxID.Equal(&transferTxID) {
		// If the current transaction is a settlement request and not the transfer transaction, then
		// there is no boomerang output, so we just use the one output.
		fundingIndex = 0
	} else {
		fundingIndex = findBoomerangIndex(transferTransaction, transfer, agentLockingScript)
	}

	if fundingIndex == 0xffffffff {
		return fmt.Errorf("Multi-Contract Transfer missing boomerang output")
	}

	currentTransaction.Lock()
	fundingOutput := currentTransaction.Output(int(fundingIndex))
	currentTransaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint32("funding_index", fundingIndex),
		logger.Uint64("funding_value", fundingOutput.Value),
	}, "Funding settlement request with output")

	if !fundingOutput.LockingScript.Equal(agentLockingScript) {
		return fmt.Errorf("Wrong locking script for funding output")
	}

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	// Find next contract
	firstContractIndex := uint32(0x0000ffff)
	nextContractIndex := uint32(0x0000ffff)
	var nextContractLockingScript bitcoin.Script
	currentFound := false
	completedContracts := make(map[bitcoin.Hash32]bool)
	outputCount := transferTransaction.OutputCount()
	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue // Instrument transfer doesn't have a contract (probably BSV transfer).
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			return errors.New("Transfer contract index out of range")
		}

		output := transferTransaction.Output(int(instrumentTransfer.ContractIndex))
		hash := bitcoin.Hash32(sha256.Sum256(output.LockingScript))

		if firstContractIndex == 0x0000ffff && output.LockingScript.Equal(agentLockingScript) {
			firstContractIndex = instrumentTransfer.ContractIndex
		}

		if !currentFound {
			completedContracts[hash] = true
			if output.LockingScript.Equal(agentLockingScript) {
				currentFound = true
			}

			continue
		}

		// Contracts can be used more than once, so ensure this contract wasn't referenced before
		// the current contract.
		_, complete := completedContracts[hash]
		if !complete {
			nextContractIndex = instrumentTransfer.ContractIndex
			nextContractLockingScript = output.LockingScript
			break
		}
	}

	if nextContractIndex == 0xffff {
		return fmt.Errorf("Next contract not found in multi-contract transfer")
	}

	messageTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := messageTx.AddInput(wire.OutPoint{Hash: currentTxID, Index: fundingIndex},
		agentLockingScript, fundingOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := messageTx.AddOutput(nextContractLockingScript, 0, true, false); err != nil {
		return errors.Wrap(err, "add next contract output")
	}

	isTest := a.IsTest()
	settlementScript, err := protocol.Serialize(settlement, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize settlement")
	}

	ra, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "agent raw address")
	}

	settlementRequest := &messages.SettlementRequest{
		Timestamp:    now,
		TransferTxId: transferTransaction.GetTxID().Bytes(),
		ContractFees: []*messages.TargetAddressField{
			{
				Address:  ra.Bytes(),
				Quantity: a.ContractFee(),
			},
		},
		Settlement: settlementScript,
	}

	payloadBuffer := &bytes.Buffer{}
	if err := settlementRequest.Serialize(payloadBuffer); err != nil {
		return errors.Wrap(err, "serialize settlement request")
	}

	message := &actions.Message{
		ReceiverIndexes: []uint32{0}, // First output is receiver of message
		MessageCode:     settlementRequest.Code(),
		MessagePayload:  payloadBuffer.Bytes(),
	}

	messageScript, err := protocol.Serialize(message, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize message")
	}

	if err := messageTx.AddOutput(messageScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add message output")
	}

	if _, err := messageTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, currentTransaction,
				NewRejectErrorWithOutputIndex(actions.RejectionsInsufficientTxFeeFunding,
					err.Error(), now, int(firstContractIndex))), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	messageTxID := *messageTx.MsgTx.TxHash()
	if _, err := a.transactions.AddRaw(ctx, messageTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, messageTxID)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("next_contract_locking_script", nextContractLockingScript),
		logger.Stringer("response_txid", messageTxID),
	}, "Sending settlement request to next contract")
	if err := a.BroadcastTx(ctx, messageTx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	return nil
}

// findBoomerangIndex returns the index to the "boomerang" output from transfer tx. It is the output
// to the contract that is not referenced/spent by the transfers. It is used to fund the offer and
// signature request messages required between multiple contracts to get a fully approved settlement
// tx.
func findBoomerangIndex(transferTransaction *state.Transaction, transfer *actions.Transfer,
	agentLockingScript bitcoin.Script) uint32 {

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	// Mark outputs referenced directly by the transfer.
	outputCount := transferTransaction.OutputCount()
	outputUsed := make([]bool, outputCount)
	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID &&
			len(instrumentTransfer.InstrumentCode) == 0 {
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			return 0xffffffff
		}

		// Output will be spent by settlement tx.
		outputUsed[instrumentTransfer.ContractIndex] = true
	}

	// Find first output matching the contract locking script that is not directly referenced by the
	// transfer.
	for i := 0; i < outputCount; i++ {
		if outputUsed[i] {
			continue
		}

		output := transferTransaction.Output(i)
		if output.LockingScript.Equal(agentLockingScript) {
			return uint32(i)
		}
	}

	return 0xffffffff
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

func (a *Agent) processSettlementRequest(ctx context.Context, transaction *state.Transaction,
	settlementRequest *messages.SettlementRequest, now uint64) error {

	// TODO Verify the input is from a contract agent involved in the transfer. --ce

	agentLockingScript := a.LockingScript()
	ra, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "agent raw address")
	}

	settlementTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	newTransferTxID, err := bitcoin.NewHash32(settlementRequest.TransferTxId)
	if err != nil {
		logger.Warn(ctx, "Invalid transfer txid in settlement request : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectError(actions.RejectionsMsgMalformed, "transfer txid invalid", now)), "reject")
	}
	transferTxID := *newTransferTxID

	settlementAction, err := protocol.Deserialize(settlementRequest.Settlement, a.IsTest())
	if err != nil {
		logger.Warn(ctx, "Failed to decode settlement from settlement request : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectError(actions.RejectionsMsgMalformed, "settlement invalid", now)), "reject")
	}

	settlement, ok := settlementAction.(*actions.Settlement)
	if !ok {
		logger.Warn(ctx, "Settlement request settlement is not a settlement : %s", err)
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectError(actions.RejectionsMsgMalformed, "settlement wrong", now)), "reject")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("transfer_txid", transferTxID),
	}, "Processing settlement request")

	transferTransaction, err := a.transactions.Get(ctx, transferTxID)
	if err != nil {
		return errors.Wrap(err, "get transfer tx")
	}

	if transferTransaction == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer tx not found")
	}
	defer a.transactions.Release(ctx, transferTxID)

	isTest := a.IsTest()
	var transfer *actions.Transfer
	transferTransaction.Lock()
	outputCount := transferTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := transferTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		tfr, ok := action.(*actions.Transfer)
		if ok {
			transfer = tfr
			break
		}
	}
	transferTransaction.Unlock()

	if transfer == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("transfer_txid", transferTxID),
		}, "Transfer action not found in transfer transaction")
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectError(actions.RejectionsMsgMalformed, "transfer tx missing transfer", now)),
			"reject")
	}

	contractOutputs := make([]*wire.TxOut, len(transfer.Instruments))
	var contractLockingScripts []bitcoin.Script
	var lastNewContractLockingScript bitcoin.Script
	var previousContractLockingScript bitcoin.Script
	transferTransaction.Lock()
	for i, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			transferTransaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				NewRejectError(actions.RejectionsMsgMalformed, "transfer tx invalid contract index",
					now)), "reject")
		}

		contractOutput := transferTransaction.Output(int(instrumentTransfer.ContractIndex))
		contractOutputs[i] = contractOutput

		if agentLockingScript.Equal(contractOutput.LockingScript) {
			if len(contractLockingScripts) == 0 {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					NewRejectError(actions.RejectionsMsgMalformed,
						"settlement request to first contract", now)), "reject")
			}

			previousContractLockingScript = contractLockingScripts[len(contractLockingScripts)-1]
		}

		found := false
		for _, contractLockingScript := range contractLockingScripts {
			if contractLockingScript.Equal(contractOutput.LockingScript) {
				found = true
				break
			}
		}

		if !found {
			lastNewContractLockingScript = contractOutput.LockingScript
			contractLockingScripts = append(contractLockingScripts, contractOutput.LockingScript)
		}
	}
	transferTransaction.Unlock()

	transaction.Lock()
	firstInputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "get first input output")
	}

	if !firstInputOutput.LockingScript.Equal(previousContractLockingScript) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			NewRejectError(actions.RejectionsMsgMalformed,
				"settlement request not from previous contract", now)), "reject")
	}

	isFinalContract := lastNewContractLockingScript.Equal(agentLockingScript)

	var balances state.Balances
	for i, instrumentTransfer := range transfer.Instruments {
		instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		exists := settlementExists(settlement, instrumentTransfer.InstrumentType,
			instrumentTransfer.InstrumentCode)

		contractOutput := contractOutputs[i]
		if !agentLockingScript.Equal(contractOutput.LockingScript) {
			isPriorContract := isFinalContract
			if !isFinalContract {
				for _, contractLockingScript := range contractLockingScripts {
					if contractLockingScript.Equal(agentLockingScript) {
						break
					}

					if contractLockingScript.Equal(contractOutput.LockingScript) {
						isPriorContract = true
						break
					}
				}
			}

			if (isFinalContract || isPriorContract) && !exists {
				logger.WarnWithFields(instrumentCtx, []logger.Field{
					logger.String("instrument_id", instrumentID),
				}, "Settlement for prior external contract doesn't exist in settlment request")
				return errors.Wrap(a.sendRejection(instrumentCtx, transaction,
					NewRejectError(actions.RejectionsMsgMalformed,
						"settlement not provided by other contract", now)), "reject")
			}

			if exists {
				if err := a.buildExternalSettlement(instrumentCtx, settlementTx,
					settlement, instrumentTransfer); err != nil {
					return errors.Wrapf(err, "build external settlement: %s", instrumentID)
				}
			}

			continue
		}

		if exists {
			logger.WarnWithFields(instrumentCtx, []logger.Field{
				logger.String("instrument_id", instrumentID),
			}, "Settlement already exists in settlment request")
			return errors.Wrap(a.sendRejection(instrumentCtx, transaction,
				NewRejectError(actions.RejectionsMsgMalformed,
					"settlement provided by other contract", now)), "reject")
		}

		instrumentSettlement, instrumentBalances, err := a.buildSettlement(instrumentCtx,
			settlementTx, settlement, transferTransaction, instrumentTransfer, contractOutputs[i],
			i, true, now)
		if err != nil {
			if rejectError, ok := err.(RejectError); ok {
				return errors.Wrap(a.sendRejection(instrumentCtx, transaction, rejectError),
					"reject")
			}
			return errors.Wrapf(err, "build settlement: %s", instrumentID)
		}

		settlement.Instruments = append(settlement.Instruments, instrumentSettlement)
		defer a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, instrumentBalances)
		balances = state.AppendBalances(balances, instrumentBalances)
	}

	settlementRequest.ContractFees = append(settlementRequest.ContractFees,
		&messages.TargetAddressField{
			Address:  ra.Bytes(),
			Quantity: a.ContractFee(),
		})

	balances.Unlock()

	return errors.New("Not Implemented")
}

func settlementExists(settlement *actions.Settlement, instrumentType string,
	instrumentCode []byte) bool {
	for _, instrumentSettlement := range settlement.Instruments {
		if instrumentSettlement.InstrumentType == instrumentType &&
			bytes.Equal(instrumentSettlement.InstrumentCode, instrumentCode) {
			return true
		}
	}

	return false
}

// buildExternalSettlement updates the settlementTx for a settlement by another contract agent. The
// settlement data should already be there.
func (a *Agent) buildExternalSettlement(ctx context.Context, settlementTx *txbuilder.TxBuilder,
	settlement *actions.Settlement, instrumentTransfer *actions.InstrumentTransferField) error {

	return errors.New("Not Implemented")
}

// buildSettlement updates the settlementTx for a settlement and creates a settlement for one
// instrument.
func (a *Agent) buildSettlement(ctx context.Context, settlementTx *txbuilder.TxBuilder,
	settlement *actions.Settlement, transferTransaction *state.Transaction,
	instrumentTransfer *actions.InstrumentTransferField, contractOutput *wire.TxOut,
	contractOutputIndex int, isMultiContract bool,
	now uint64) (*actions.InstrumentSettlementField, state.Balances, error) {

	agentLockingScript := a.LockingScript()
	adminLockingScript := a.AdminLockingScript()

	transferTxID := transferTransaction.GetTxID()
	contractInputIndex, err := addResponseInput(settlementTx, transferTxID, contractOutput,
		contractOutputIndex)
	if err != nil {
		return nil, nil, errors.Wrap(err, "add response input")
	}

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

	a.contract.Lock()
	instrument := a.contract.GetInstrument(instrumentCode)
	a.contract.Unlock()

	if instrument == nil {
		logger.Warn(ctx, "Instrument not found: %s", instrumentCode)
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound, "",
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
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound, "",
			now, int(instrumentTransfer.ContractIndex))
	}

	logger.Info(ctx, "Processing transfer")

	// Check if instrument is frozen.
	if isFrozen {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is frozen")
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentFrozen, "", now,
			int(instrumentTransfer.ContractIndex))
	}

	// Check if instrument is expired or event is over.
	if isExpired {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is expired")
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted,
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
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"sender quantity is zero", now, int(instrumentTransfer.ContractIndex))
		}

		senderQuantity += sender.Quantity

		inputOutput, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			logger.Warn(ctx, "Invalid sender index : %s", err)
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
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
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"receiver quantity is zero", now, int(instrumentTransfer.ContractIndex))
		}

		receiverQuantity += receiver.Quantity

		ra, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address : %s", err)
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid receiver address: %s", err), now,
				int(instrumentTransfer.ContractIndex))
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address script : %s", err)
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
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
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted, "",
			now, int(instrumentTransfer.ContractIndex))
	}

	if senderQuantity != receiverQuantity {
		logger.Warn(ctx,
			"Sender and receiver quantity do not match : sender %d, receiver %d",
			senderQuantity, receiverQuantity)
		return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
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

		if err := balance.AddPendingDebit(sender.Quantity); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", sender.Quantity),
			}, "Failed to add debit : %s", err)

			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

			// TODO This can also be actions.RejectionsHoldingsFrozen. --ce
			return nil, nil, NewRejectErrorFull(actions.RejectionsInsufficientQuantity,
				fmt.Sprintf("sender %d: %s", i, err), now, int(sender.Index),
				int(instrumentTransfer.ContractIndex))
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

		if err := balance.AddPendingCredit(receiver.Quantity); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", receiver.Quantity),
			}, "Failed to add credit : %s", err)
			balances.RevertPending(&transferTxID)
			balances.Unlock()
			a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
			return nil, nil, NewRejectErrorWithOutputIndex(actions.RejectionsInsufficientQuantity,
				err.Error(), now, int(instrumentTransfer.ContractIndex))
		}
	}

	instrumentSettlement := &actions.InstrumentSettlementField{
		ContractIndex:  contractInputIndex,
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode[:],
	}

	if err := populateTransferSettlement(settlementTx, instrumentSettlement,
		balances); err != nil {
		balances.RevertPending(&transferTxID)
		balances.Unlock()
		a.balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
		return nil, nil, errors.Wrap(err, "populate settlement")
	}

	balances.FinalizePending(&transferTxID, isMultiContract)

	return instrumentSettlement, balances, nil
}

func (a *Agent) processSignatureRequest(ctx context.Context, transaction *state.Transaction,
	settlementRequest *messages.SignatureRequest, now uint64) error {

	return errors.New("Not Implemented")
}
