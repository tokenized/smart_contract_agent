package agents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/headers"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processTransfer(ctx context.Context, transaction *transactions.Transaction,
	transfer *actions.Transfer, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	// Verify appropriate output belongs to this contract.
	agentLockingScript := a.LockingScript()

	txid := transaction.GetTxID()
	transferContracts, err := parseTransferContracts(transaction, transfer, agentLockingScript)
	if err != nil {
		if errors.Cause(err) == ErrNotRelevant {
			logger.Warn(ctx, "Transfer not relevant to this contract agent")
			return nil, nil // Not for this contract
		}

		return nil, errors.Wrap(err, "parse contracts")
	}

	if !transferContracts.IsFirstContract() {
		logger.Info(ctx, "Waiting for settlement request message to process transfer")
		return nil, nil // Wait for settlement request message
	}

	now := a.Now()

	if err := a.CheckContractIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	// TODO Verify boomerang output has enough funding. --ce
	// if transferContracts.IsMultiContract() {

	// }

	headers := headers.NewHeadersCache(a.headers)
	requiresIdentityOracles, err := a.RequiresIdentityOracles(ctx)
	if err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	instruments := make([]*state.Instrument, len(transfer.Instruments))
	instrumentCodes := make([]state.InstrumentCode, len(transfer.Instruments))
	allBalances := make(state.BalanceSet, len(transfer.Instruments))
	allSenderLockingScripts := make([][]bitcoin.Script, len(transfer.Instruments))
	allReceiverLockingScripts := make([][]bitcoin.Script, len(transfer.Instruments))
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentTransfer.InstrumentCode)

		instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
		if err != nil {
			return nil, errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound,
				"", int(instrumentTransfer.ContractIndex))
		}
		defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

		balances, senderLockingScripts, receiverLockingScripts, err := a.initiateInstrumentTransferBalances(instrumentCtx,
			transaction, instrument, instrumentCode, instrumentTransfer, headers)
		if err != nil {
			return nil, errors.Wrapf(err, "build settlement: %s", instrumentID)
		}

		instruments[index] = instrument
		instrumentCodes[index] = instrumentCode
		allBalances[index] = balances
		allSenderLockingScripts[index] = senderLockingScripts
		allReceiverLockingScripts[index] = receiverLockingScripts

		defer a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)
	}

	lockerResponseChannel := a.locker.AddRequest(allBalances)
	lockerResponse := <-lockerResponseChannel
	switch v := lockerResponse.(type) {
	case uint64:
		now = v
	case error:
		return nil, errors.Wrap(v, "locker")
	}
	defer allBalances.Unlock()

	// Verify expiry.
	if transfer.OfferExpiry != 0 && now > transfer.OfferExpiry {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("expiry", int64(transfer.OfferExpiry)),
			logger.Timestamp("now", int64(now)),
		}, "Transfer offer expired")
		return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsTransferExpired, "",
			transferContracts.FirstContractOutputIndex)
	}

	config := a.Config()
	settlementTx := txbuilder.NewTxBuilder(config.FeeRate, config.DustFeeRate)
	settlement := &actions.Settlement{
		Timestamp: now,
	}

	for index, instrumentTransfer := range transfer.Instruments {
		instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			if len(instrumentTransfer.InstrumentCode) != 0 {
				allBalances.Revert(txid)
				logger.Warn(instrumentCtx, "Bitcoin instrument with instrument code")
				return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
					"bitcoin transfer with instrument code",
					transferContracts.FirstContractOutputIndex)
			}

			if err := a.buildBitcoinTransfer(instrumentCtx, transaction, settlementTx,
				instrumentTransfer); err != nil {
				allBalances.Revert(txid)
				return nil, errors.Wrap(err, "build bitcoin transfer")
			}

			continue
		}

		if !agentLockingScript.Equal(transferContracts.Outputs[index].LockingScript) {
			continue
		}

		instrumentSettlement, err := a.buildInstrumentSettlement(instrumentCtx, allBalances[index],
			allSenderLockingScripts[index], allReceiverLockingScripts[index], settlementTx, txid,
			instruments[index], instrumentCodes[index], instrumentTransfer,
			transferContracts.Outputs[index], transferContracts.IsMultiContract(),
			requiresIdentityOracles, headers, now)
		if err != nil {
			allBalances.Revert(txid)
			return nil, errors.Wrapf(err, "build settlement: %s", instrumentID)
		}

		settlement.Instruments = append(settlement.Instruments, instrumentSettlement)
	}

	if err := settlement.Validate(); err != nil {
		allBalances.Revert(txid)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	settlementScript, err := protocol.Serialize(settlement, config.IsTest)
	if err != nil {
		allBalances.Revert(txid)
		return nil, errors.Wrap(err, "serialize settlement")
	}

	settlementScriptOutputIndex := len(settlementTx.Outputs)
	if err := settlementTx.AddOutput(settlementScript, 0, false, false); err != nil {
		allBalances.Revert(txid)
		return nil, errors.Wrap(err, "add settlement output")
	}

	// Add the exchange fee
	if transfer.ExchangeFee > 0 {
		ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
		if err != nil {
			allBalances.Revert(txid)
			logger.Warn(ctx, "Invalid exchange fee address : %s", err)
			return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(),
				transferContracts.FirstContractOutputIndex)
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			allBalances.Revert(txid)
			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed, err.Error(),
				transferContracts.FirstContractOutputIndex)
		}

		if err := settlementTx.AddOutput(lockingScript, transfer.ExchangeFee, false,
			false); err != nil {
			allBalances.Revert(txid)
			return nil, errors.Wrap(err, "add exchange fee")
		}
	}

	// Add the contract fee
	if a.ContractFee() > 0 {
		if err := settlementTx.AddOutput(a.FeeLockingScript(), a.ContractFee(), true,
			false); err != nil {
			allBalances.Revert(txid)
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := settlementTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		allBalances.Revert(txid)
		return nil, errors.Wrap(err, "set change")
	}

	if transferContracts.IsMultiContract() {
		// Send a settlement request to the next contract.
		etx, err := a.createSettlementRequest(ctx, transaction, transaction, outputIndex, transfer,
			transferContracts, allBalances, settlement)
		if err != nil {
			return etx, errors.Wrap(err, "send settlement request")
		}

		return etx, nil
	}

	// Sign settlement tx.
	if _, err := settlementTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		allBalances.Revert(txid)
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInsufficientTxFeeFunding,
				err.Error(), transferContracts.FirstContractOutputIndex)
		}

		return nil, errors.Wrap(err, "sign")
	}

	etx, err := a.completeSettlement(ctx, transaction, outputIndex, txid, settlementTx,
		settlementScriptOutputIndex, allBalances, now)
	if err != nil {
		allBalances.Revert(txid)
		return etx, errors.Wrap(err, "complete settlement")
	}

	return etx, nil
}

func (a *Agent) buildBitcoinTransfer(ctx context.Context, transferTransaction *transactions.Transaction,
	settlementTx *txbuilder.TxBuilder, instrumentTransfer *actions.InstrumentTransferField) error {

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	quantity := uint64(0)
	var usedInputs []uint32
	for _, sender := range instrumentTransfer.InstrumentSenders {
		for _, used := range usedInputs {
			if used == sender.Index {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					"input used as bitcoin sender more than once")
			}
		}
		usedInputs = append(usedInputs, sender.Index)

		output, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
		}

		if sender.Quantity >= output.Value {
			return platform.NewRejectError(actions.RejectionsInsufficientValue,
				"sender input value less than quantity")
		}

		quantity += output.Value
	}

	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		ra, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			return errors.Wrapf(err, "locking script %d", i)
		}

		if err := settlementTx.AddOutput(lockingScript, receiver.Quantity, false,
			false); err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
		}

		if receiver.Quantity > quantity {
			return platform.NewRejectError(actions.RejectionsInsufficientValue,
				"sender quantity less than receiver")
		}

		quantity -= receiver.Quantity
	}

	if quantity != 0 {
		return platform.NewRejectError(actions.RejectionsInsufficientValue,
			"sender quantity more than receiver")
	}

	return nil
}

func (a *Agent) completeSettlement(ctx context.Context, transferTransaction *transactions.Transaction,
	transferOutputIndex int, transferTxID bitcoin.Hash32, settlementTx *txbuilder.TxBuilder,
	settlementScriptOutputIndex int, balances state.BalanceSet,
	now uint64) (*expanded_tx.ExpandedTx, error) {

	settlementTxID := *settlementTx.MsgTx.TxHash()
	settlementTransaction, err := a.transactions.AddRaw(ctx, settlementTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, settlementTxID)

	// Settle balances regardless of tx acceptance by the network as the agent is the single source
	// of truth.
	balances.Settle(transferTxID, settlementTxID, now)

	// Set settlement tx as processed since all the balances were just settled.
	settlementTransaction.Lock()
	settlementTransaction.SetProcessed(a.ContractHash(), settlementScriptOutputIndex)
	settlementTransaction.Unlock()

	transferTransaction.Lock()
	transferTransaction.AddResponseTxID(a.ContractHash(), transferOutputIndex, settlementTxID)
	transferTx := transferTransaction.Tx.Copy()
	transferTransaction.Unlock()

	etx, err := buildExpandedTx(settlementTx.MsgTx, []*wire.MsgTx{transferTx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", settlementTxID),
	}, "Responding with settlement")

	if err := a.AddResponse(ctx, transferTxID, balances.LockingScripts(), false, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	return etx, nil
}

func (a *Agent) processSettlement(ctx context.Context, transaction *transactions.Transaction,
	settlement *actions.Settlement, outputIndex int) error {

	transferTxID, lockingScripts, err := a.applySettlements(ctx, transaction,
		settlement.Instruments, settlement.Timestamp)
	if err != nil {
		return errors.Wrap(err, "apply settlements")
	}

	if transferTxID != nil {
		transaction.Lock()
		etx, err := a.transactions.ExpandedTx(ctx, transaction)
		transaction.Unlock()
		if err != nil {
			return errors.Wrap(err, "expand tx")
		}

		if _, err := a.addResponseTxID(ctx, *transferTxID, etx.TxID()); err != nil {
			return errors.Wrap(err, "add response txid")
		}

		if err := a.AddResponse(ctx, *transferTxID, lockingScripts, false, etx); err != nil {
			return errors.Wrap(err, "respond")
		}
	}

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) processRectificationSettlement(ctx context.Context, transaction *transactions.Transaction,
	settlement *actions.RectificationSettlement, outputIndex int) error {

	transferTxID, lockingScripts, err := a.applySettlements(ctx, transaction,
		settlement.Instruments, settlement.Timestamp)
	if err != nil {
		return errors.Wrap(err, "apply settlements")
	}

	if transferTxID != nil {
		if _, err := a.addResponseTxID(ctx, *transferTxID, transaction.GetTxID()); err != nil {
			return errors.Wrap(err, "add response txid")
		}
	}

	transaction.Lock()
	etx, err := a.transactions.ExpandedTx(ctx, transaction)
	transaction.Unlock()
	if err != nil {
		return errors.Wrap(err, "expand tx")
	}

	if len(lockingScripts) > 0 {
		if err := postToLockingScriptSubscriptions(ctx, a.caches, a.LockingScript(), lockingScripts,
			etx); err != nil {
			return errors.Wrap(err, "post settlement")
		}
	}

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) applySettlements(ctx context.Context, transaction *transactions.Transaction,
	settlements []*actions.InstrumentSettlementField,
	timestamp uint64) (*bitcoin.Hash32, []bitcoin.Script, error) {

	agentLockingScript := a.LockingScript()

	transaction.Lock()
	defer transaction.Unlock()

	txid := transaction.TxID()
	outputCount := transaction.OutputCount()
	var lockingScripts []bitcoin.Script
	var transferTxID *bitcoin.Hash32

	// Update one instrument at a time.
	allAddedBalances := make(state.BalanceSet, len(settlements))
	allBalances := make(state.BalanceSet, len(settlements))
	for index, instrumentSettlement := range settlements {
		instrumentID, _ := protocol.InstrumentIDForSettlement(instrumentSettlement)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		if int(instrumentSettlement.ContractIndex) >= transaction.InputCount() {
			logger.Error(instrumentCtx, "Invalid settlement contract index: %d >= %d",
				instrumentSettlement.ContractIndex, transaction.InputCount())
			return transferTxID, lockingScripts, nil
		}

		if transferTxID == nil {
			ttxid := transaction.Input(int(instrumentSettlement.ContractIndex)).PreviousOutPoint.Hash
			transferTxID = &ttxid
		}

		contractInputOutput, err := transaction.InputOutput(int(instrumentSettlement.ContractIndex))
		if err != nil {
			return transferTxID, lockingScripts, errors.Wrap(err, "contract input locking script")
		}

		if !contractInputOutput.LockingScript.Equal(agentLockingScript) {
			continue
		}

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], instrumentSettlement.InstrumentCode)

		instrument, err := a.caches.Instruments.Get(instrumentCtx, agentLockingScript,
			instrumentCode)
		if err != nil {
			return transferTxID, lockingScripts, errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			logger.Error(instrumentCtx, "Instrument not found: %s", instrumentCode)
			return transferTxID, lockingScripts, nil
		}
		a.caches.Instruments.Release(instrumentCtx, agentLockingScript, instrumentCode)

		logger.Info(instrumentCtx, "Processing settlement")

		// Build balances based on the instrument's settlement quantities.
		balances := make(state.Balances, len(instrumentSettlement.Settlements))
		for i, settle := range instrumentSettlement.Settlements {
			if int(settle.Index) >= outputCount {
				logger.ErrorWithFields(instrumentCtx, []logger.Field{
					logger.Int("settlement_index", i),
					logger.Uint32("output_index", settle.Index),
					logger.Int("output_count", outputCount),
				}, "Invalid settlement output index")
				return transferTxID, lockingScripts, nil
			}

			lockingScript := transaction.Output(int(settle.Index)).LockingScript
			lockingScripts = append(lockingScripts, lockingScript)

			balances[i] = &state.Balance{
				LockingScript: lockingScript,
				Quantity:      settle.Quantity,
				Timestamp:     timestamp,
				TxID:          &txid,
			}
		}
		allBalances[index] = balances

		// Add the balances to the cache.
		addedBalances, err := a.caches.Balances.AddMulti(instrumentCtx, agentLockingScript,
			instrumentCode, balances)
		if err != nil {
			return transferTxID, lockingScripts, errors.Wrap(err, "add balances")
		}
		defer a.caches.Balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode,
			addedBalances)

		allAddedBalances[index] = addedBalances
	}

	lockerResponseChannel := a.locker.AddRequest(allAddedBalances)
	lockerResponse := <-lockerResponseChannel
	switch v := lockerResponse.(type) {
	case uint64:
		// now = v // timestamp
	case error:
		return transferTxID, lockingScripts, errors.Wrap(v, "locker")
	}
	defer allAddedBalances.Unlock()

	for index, instrumentSettlement := range settlements {
		instrumentID, _ := protocol.InstrumentIDForSettlement(instrumentSettlement)
		instrumentCtx := logger.ContextWithLogFields(ctx,
			logger.String("instrument_id", instrumentID))

		balances := allBalances[index]
		addedBalances := allAddedBalances[index]
		if len(addedBalances) == 0 {
			continue
		}

		// Update any balances that weren't new and therefore weren't updated by the "add".
		for i, balance := range balances {
			addedBalance := addedBalances[i]
			if balance == addedBalance {
				logger.InfoWithFields(instrumentCtx, []logger.Field{
					logger.Timestamp("timestamp", int64(addedBalance.Timestamp)),
					logger.Timestamp("existing_timestamp", int64(timestamp)),
					logger.Stringer("locking_script", balance.LockingScript),
					logger.Uint64("quantity", balance.Quantity),
				}, "New hard balance settlement")
				continue // balance was new and is already up to date from the add.
			}

			// If the balance doesn't match then it already existed and must be updated.
			if timestamp < addedBalance.Timestamp {
				logger.InfoWithFields(instrumentCtx, []logger.Field{
					logger.Timestamp("timestamp", int64(addedBalance.Timestamp)),
					logger.Timestamp("old_timestamp", int64(timestamp)),
					logger.Stringer("locking_script", balance.LockingScript),
					logger.Uint64("quantity", addedBalance.Quantity),
					logger.Uint64("old_quantity", balance.Quantity),
				}, "Older settlement ignored")
				continue
			}

			// Update balance
			if addedBalance.Settle(*transferTxID, txid, timestamp) {
				logger.InfoWithFields(instrumentCtx, []logger.Field{
					logger.Timestamp("timestamp", int64(addedBalance.Timestamp)),
					logger.Timestamp("existing_timestamp", int64(timestamp)),
					logger.Stringer("locking_script", balance.LockingScript),
					logger.Uint64("settlement_quantity", balance.Quantity),
					logger.Uint64("quantity", addedBalance.Quantity),
				}, "Applied prior balance adjustment settlement")
				continue
			}

			logger.InfoWithFields(instrumentCtx, []logger.Field{
				logger.Timestamp("timestamp", int64(addedBalance.Timestamp)),
				logger.Timestamp("existing_timestamp", int64(timestamp)),
				logger.Stringer("locking_script", balance.LockingScript),
				logger.Uint64("previous_quantity", addedBalance.Quantity),
				logger.Uint64("quantity", balance.Quantity),
			}, "Applying hard balance settlement")

			addedBalance.Quantity = balance.Quantity
			addedBalance.Timestamp = timestamp
			addedBalance.TxID = &txid
			addedBalance.MarkModified()
		}
	}

	return transferTxID, lockingScripts, nil
}

// populateTransferSettlement adds all the new balances to the transfer settlement.
func populateTransferSettlement(ctx context.Context, tx *txbuilder.TxBuilder,
	transferSettlement *actions.InstrumentSettlementField, balances state.Balances) error {

	for i, balance := range balances {
		index, err := addDustLockingScript(tx, balance.LockingScript)
		if err != nil {
			return errors.Wrapf(err, "add locking script %d", i)
		}

		quantity := balance.SettlePendingQuantity()
		transferSettlement.Settlements = append(transferSettlement.Settlements,
			&actions.QuantityIndexField{
				Quantity: quantity,
				Index:    index,
			})

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("locking_script", balance.LockingScript),
			logger.Uint64("quantity", quantity),
		}, "Settlement balance")
	}

	return nil
}

type TransferContracts struct {
	// LockingScripts is the list of contract agent locking scripts in order they should be
	// processed.
	LockingScripts []bitcoin.Script
	CurrentIndex   int // Index of LockingScripts that matches current contract agent

	// FirstContractOutputIndex is the index of the output of the first contract. It will be zero
	// unless bitcoin is the first transfer.
	FirstContractOutputIndex int

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
func parseTransferContracts(transferTransaction *transactions.Transaction, transfer *actions.Transfer,
	currentLockingScript bitcoin.Script) (*TransferContracts, error) {

	count := len(transfer.Instruments)
	if count == 0 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"transfer has no instruments")
	}

	result := &TransferContracts{
		CurrentIndex:             -1,
		FirstContractOutputIndex: -1,
		Outputs:                  make([]*wire.TxOut, count),
		OutputIndexes:            make([]int, count),
	}

	transferTransaction.Lock()
	defer transferTransaction.Unlock()

	outputCount := transferTransaction.OutputCount()
	var instrumentCodes [][]byte
	for i, instrumentTransfer := range transfer.Instruments {
		if len(instrumentTransfer.InstrumentSenders) == 0 {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "missing senders")
		}

		if len(instrumentTransfer.InstrumentReceivers) == 0 {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "missing receivers")
		}

		var instrumentCode []byte
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			instrumentCode = nil
		} else {
			instrumentCode = instrumentTransfer.InstrumentCode
		}

		for _, instrument := range instrumentCodes {
			if bytes.Equal(instrument, instrumentCode) {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"duplicate instrument")
			}
		}

		instrumentCodes = append(instrumentCodes, instrumentCode)

		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if result.FirstContractOutputIndex == -1 {
			result.FirstContractOutputIndex = i
		}

		if int(instrumentTransfer.ContractIndex) >= outputCount {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"transfer tx invalid contract index")
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

func (a *Agent) initiateInstrumentTransferBalances(ctx context.Context,
	transferTransaction *transactions.Transaction, instrument *state.Instrument,
	instrumentCode state.InstrumentCode, instrumentTransfer *actions.InstrumentTransferField,
	headers BlockHeaders) (state.Balances, []bitcoin.Script, []bitcoin.Script, error) {

	agentLockingScript := a.LockingScript()
	adminLockingScript := a.AdminLockingScript()

	agentAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "agent address")
	}

	instrument.Lock()
	instrumentType := string(instrument.InstrumentType[:])
	transfersPermitted := instrument.TransfersPermitted()
	instrument.Unlock()

	if instrumentTransfer.InstrumentType != instrumentType {
		logger.Warn(ctx, "Wrong instrument type: %s (should be %s)",
			instrumentTransfer.InstrumentType, instrument.InstrumentType)
		return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotFound,
			"", int(instrumentTransfer.ContractIndex))
	}

	logger.Info(ctx, "Initiating transfer settlement")

	identityOracles, err := a.GetIdentityOracles(ctx)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "get identity oracles")
	}

	// Get relevant balances.
	var addBalances state.Balances
	var senderLockingScripts []bitcoin.Script
	var senderQuantity uint64
	onlyFromAdmin := true
	onlyToAdmin := true
	exists := make(map[bitcoin.Hash32]bool)
	for _, sender := range instrumentTransfer.InstrumentSenders {
		if sender.Quantity == 0 {
			logger.Warn(ctx, "Sender quantity is zero")
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"sender quantity is zero", int(instrumentTransfer.ContractIndex))
		}

		senderQuantity += sender.Quantity

		authorizingUnlockingScript := transferTransaction.Input(int(sender.Index)).UnlockingScript
		inputOutput, err := transferTransaction.InputOutput(int(sender.Index))
		if err != nil {
			logger.Warn(ctx, "Invalid sender index : %s", err)
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid sender index: %d", sender.Index),
				int(instrumentTransfer.ContractIndex))
		}

		if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
			return nil, nil, nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll,
				err.Error())
		} else if !isSigHashAll {
			return nil, nil, nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll,
				"")
		}

		if !adminLockingScript.Equal(inputOutput.LockingScript) {
			onlyFromAdmin = false
		}

		hash := bitcoin.Hash32(sha256.Sum256(inputOutput.LockingScript))
		if _, ok := exists[hash]; ok {
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("duplicate sender script: %d", sender.Index),
				int(instrumentTransfer.ContractIndex))
		}
		exists[hash] = true

		senderLockingScripts = append(senderLockingScripts, inputOutput.LockingScript)
		addBalances = state.AppendZeroBalance(addBalances, inputOutput.LockingScript)
	}

	var receiverLockingScripts []bitcoin.Script
	var receiverQuantity uint64
	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		if receiver.Quantity == 0 {
			logger.Warn(ctx, "Receiver quantity is zero")
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				"receiver quantity is zero", int(instrumentTransfer.ContractIndex))
		}

		receiverQuantity += receiver.Quantity

		receiverAddress, err := bitcoin.DecodeRawAddress(receiver.Address)
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address : %s", err)
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid receiver address: %s", err),
				int(instrumentTransfer.ContractIndex))
		}

		lockingScript, err := receiverAddress.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid receiver address script : %s", err)
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("invalid receiver address script: %s", err),
				int(instrumentTransfer.ContractIndex))
		}

		if len(identityOracles) > 0 {
			// Verify receiver identity oracle signatures.
			if err := verifyIdentityOracleReceiverSignature(ctx, agentAddress,
				instrumentTransfer.InstrumentCode, identityOracles, receiver, receiverAddress,
				headers); err != nil {
				return nil, nil, nil, errors.Wrap(err, "identity oracle signature")
			}
		}

		if !adminLockingScript.Equal(lockingScript) {
			onlyToAdmin = false
		}

		hash := bitcoin.Hash32(sha256.Sum256(lockingScript))
		if _, ok := exists[hash]; ok {
			return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
				fmt.Sprintf("duplicate receiver script: %d", i),
				int(instrumentTransfer.ContractIndex))
		}
		exists[hash] = true

		receiverLockingScripts = append(receiverLockingScripts, lockingScript)
		addBalances = state.AppendZeroBalance(addBalances, lockingScript)
	}

	if !transfersPermitted && !onlyFromAdmin && !onlyToAdmin {
		logger.Warn(ctx, "Transfers not permitted")
		return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted,
			"", int(instrumentTransfer.ContractIndex))
	}

	if senderQuantity != receiverQuantity {
		logger.Warn(ctx,
			"Sender and receiver quantity do not match : sender %d, receiver %d",
			senderQuantity, receiverQuantity)
		return nil, nil, nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsMsgMalformed,
			"sender quantity != receiver quantity", int(instrumentTransfer.ContractIndex))
	}

	balances, err := a.caches.Balances.AddMulti(ctx, agentLockingScript, instrumentCode,
		addBalances)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "add balances")
	}

	return balances, senderLockingScripts, receiverLockingScripts, nil
}

// buildInstrumentSettlement updates the settlementTx for a settlement and creates a settlement for
// one instrument.
func (a *Agent) buildInstrumentSettlement(ctx context.Context, balances state.Balances,
	senderLockingScripts, receiverLockingScripts []bitcoin.Script,
	settlementTx *txbuilder.TxBuilder, transferTxID bitcoin.Hash32, instrument *state.Instrument,
	instrumentCode state.InstrumentCode, instrumentTransfer *actions.InstrumentTransferField,
	contractOutput *wire.TxOut, isMultiContract, requiresIdentityOracles bool, headers BlockHeaders,
	now uint64) (*actions.InstrumentSettlementField, error) {

	contractInputIndex, err := addResponseInput(settlementTx, transferTxID, contractOutput,
		int(instrumentTransfer.ContractIndex))
	if err != nil {
		return nil, errors.Wrap(err, "add response input")
	}

	instrument.Lock()
	isFrozen := instrument.IsFrozen(now)
	isExpired := instrument.IsExpired(now)
	instrument.Unlock()

	logger.Info(ctx, "Building transfer settlement")

	// Check if instrument is frozen.
	if isFrozen {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is frozen")
		return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentFrozen,
			"", int(instrumentTransfer.ContractIndex))
	}

	// Check if instrument is expired or event is over.
	if isExpired {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("now", int64(now)),
		}, "Instrument is expired")
		return nil, platform.NewRejectErrorWithOutputIndex(actions.RejectionsInstrumentNotPermitted,
			"expired", int(instrumentTransfer.ContractIndex))
	}

	for i, sender := range instrumentTransfer.InstrumentSenders {
		lockingScript := senderLockingScripts[i]

		balance := balances.Find(lockingScript)
		if balance == nil {
			return nil, fmt.Errorf("Missing balance for sender %d : %s", i, lockingScript)
		}

		if err := balance.AddPendingDebit(sender.Quantity, now); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", sender.Quantity),
			}, "Failed to add debit : %s", err)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.InputIndex = int(sender.Index)
				rejectError.OutputIndex = int(instrumentTransfer.ContractIndex)
				rejectError.Message = fmt.Sprintf("sender %d: %s", i, rejectError.Message)
			}

			return nil, errors.Wrap(err, "add debit")
		}
	}

	for i, receiver := range instrumentTransfer.InstrumentReceivers {
		if requiresIdentityOracles && receiver.OracleSigExpiry != 0 &&
			now > receiver.OracleSigExpiry {
			return nil, platform.NewRejectError(actions.RejectionsTransferExpired,
				"identity oracle signature expired")
		}

		lockingScript := receiverLockingScripts[i]
		balance := balances.Find(lockingScript)
		if balance == nil {
			return nil, fmt.Errorf("Missing balance for receiver %d : %s", i, lockingScript)
		}

		if err := balance.AddPendingCredit(receiver.Quantity, now); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("locking_script", lockingScript),
				logger.Uint64("quantity", receiver.Quantity),
			}, "Failed to add credit : %s", err)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.OutputIndex = int(instrumentTransfer.ContractIndex)
				rejectError.Message = fmt.Sprintf("receiver %d: %s", i, rejectError.Message)
			}

			return nil, errors.Wrap(err, "add credit")
		}
	}

	instrumentSettlement := &actions.InstrumentSettlementField{
		ContractIndex:  contractInputIndex,
		InstrumentType: instrumentTransfer.InstrumentType,
		InstrumentCode: instrumentCode[:],
	}

	if err := populateTransferSettlement(ctx, settlementTx, instrumentSettlement,
		balances); err != nil {
		return nil, errors.Wrap(err, "populate settlement")
	}

	balances.SettlePending(transferTxID, isMultiContract)
	return instrumentSettlement, nil
}

func verifyIdentityOracleReceiverSignature(ctx context.Context,
	contractAddress bitcoin.RawAddress, instrumentCode []byte, identityOracles []*IdentityOracle,
	receiver *actions.InstrumentReceiverField, receiverAddress bitcoin.RawAddress,
	headers BlockHeaders) error {

	if receiver.OracleSigAlgorithm == 0 {
		return platform.NewRejectError(actions.RejectionsInvalidSignature,
			"missing identity oracle signature")
	}

	var oraclePublicKey *bitcoin.PublicKey
	for _, oracle := range identityOracles {
		if oracle.Index == int(receiver.OracleIndex) {
			oraclePublicKey = &oracle.PublicKey
			break
		}
	}

	if oraclePublicKey == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			"invalid identity oracle index")
	}

	signature, err := bitcoin.SignatureFromBytes(receiver.OracleConfirmationSig)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsInvalidSignature,
			fmt.Sprintf("invalid identity oracle signature encoding: %s", err))
	}

	hash, err := headers.BlockHash(ctx, int(receiver.OracleSigBlockHeight))
	if err != nil {
		return errors.Wrap(err, "get block hash")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("oracle_public_key", oraclePublicKey),
		logger.Int("block_height", int(receiver.OracleSigBlockHeight)),
		logger.Stringer("block_hash", hash),
	}, "Verifying identity oracle receiver signature")

	sigHash, err := protocol.TransferOracleSigHash(ctx, contractAddress, instrumentCode,
		receiverAddress, *hash, receiver.OracleSigExpiry, 1)
	if err != nil {
		return errors.Wrap(err, "sig hash")
	}

	if !signature.Verify(*sigHash, *oraclePublicKey) {
		return platform.NewRejectError(actions.RejectionsInvalidSignature,
			"invalid identity oracle signature")
	}

	return nil
}
