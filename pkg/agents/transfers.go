package agents

import (
	"bytes"
	"context"
	"fmt"

	channels_expanded_tx "github.com/tokenized/channels/expanded_tx"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
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

	txid := transaction.TxID()
	adminLockingScript := a.AdminLockingScript()

	transaction.Lock()
	defer transaction.Unlock()

	responseTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	// Check if this is a single contract settlement or if this agent is for the master contract,
	// meaning this contract acts first and last on a multi-contract settlement.
	isRelevant := false
	isSingleContract := true
	isMasterContract := false
	firstContractIndex := -1
	firstInstrument := true
	for index, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= transaction.OutputCount() {
			logger.Warn(ctx, "Invalid transfer contract index: %d >= %d",
				instrumentTransfer.ContractIndex, transaction.OutputCount())
			return nil
		}

		contractOutput := transaction.Output(int(instrumentTransfer.ContractIndex))
		if agentLockingScript.Equal(contractOutput.LockingScript) {
			isRelevant = true
			if firstInstrument {
				isMasterContract = true
			}
			if firstContractIndex == -1 {
				firstContractIndex = index
			}
		} else {
			isSingleContract = false
		}
		firstInstrument = false
	}

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
		return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
			actions.RejectionsContractExpired, "", now), "reject")
	}

	// Verify expiry.
	if transfer.OfferExpiry != 0 && now > transfer.OfferExpiry {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("expiry", int64(transfer.OfferExpiry)),
			logger.Timestamp("now", int64(now)),
		}, "Transfer offer expired")
		return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
			actions.RejectionsTransferExpired, "", now), "reject")
	}

	// Verify there are no duplicate instruments.
	var instruments [][]byte
	for _, instrumentTransfer := range transfer.Instruments {
		if len(instrumentTransfer.InstrumentSenders) == 0 {
			logger.Warn(ctx, "Transfer is missing senders")
			return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
				actions.RejectionsMsgMalformed, "missing senders", now), "reject")
		}

		if len(instrumentTransfer.InstrumentReceivers) == 0 {
			logger.Warn(ctx, "Transfer is missing receivers")
			return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
				actions.RejectionsMsgMalformed, "missing receivers", now), "reject")
		}

		for _, instrument := range instruments {
			if bytes.Equal(instrument, instrumentTransfer.InstrumentCode) {
				instrumentID, _ := protocol.InstrumentIDForTransfer(instrumentTransfer)
				logger.WarnWithFields(ctx, []logger.Field{
					logger.String("instrument_id", instrumentID),
				}, "Duplicate instrument in transfer")
				return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
					actions.RejectionsMsgMalformed, "duplicate instrument", now), "reject")
			}
		}

		instruments = append(instruments, instrumentTransfer.InstrumentCode)
	}

	// Process transfers.
	settlement := &actions.Settlement{
		Timestamp: now,
	}

	var allBalances state.Balances
	var allLockingScripts []bitcoin.Script
	for _, instrumentTransfer := range transfer.Instruments {
		if instrumentTransfer.InstrumentType == protocol.BSVInstrumentID {
			if len(instrumentTransfer.InstrumentCode) != 0 {
				logger.Warn(ctx, "Bitcoin instrument with instrument code")
				return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
					actions.RejectionsMsgMalformed, "bitcoin transfer with instrument code", now),
					"reject")
			}

			// TODO Handle bitcoin transfers. --ce
			logger.Info(ctx, "Bitcoin transfer")
			continue
		}

		if int(instrumentTransfer.ContractIndex) >= transaction.OutputCount() {
			logger.Warn(ctx, "Invalid transfer contract index: %d >= %d",
				instrumentTransfer.ContractIndex, transaction.OutputCount())
			return nil
		}

		contractOutput := transaction.Output(int(instrumentTransfer.ContractIndex))
		if !agentLockingScript.Equal(contractOutput.LockingScript) {
			continue
		}

		if err := addResponseInput(responseTx, transaction.GetMsgTx(),
			int(instrumentTransfer.ContractIndex)); err != nil {
			return errors.Wrap(err, "add response input")
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
		instrument := a.contract.GetInstrument(instrumentCode)
		a.contract.Unlock()

		if instrument == nil {
			logger.Warn(instrumentCtx, "Instrument not found: %s", instrumentCode)
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsInstrumentNotFound, "",
				now), "reject")
		}

		instrument.Lock()
		instrumentType := string(instrument.InstrumentType[:])
		transfersPermitted := instrument.TransfersPermitted()
		isFrozen := instrument.IsFrozen(now)
		isExpired := instrument.IsExpired(now)
		instrument.Unlock()

		if instrumentTransfer.InstrumentType != instrumentType {
			logger.Warn(instrumentCtx, "Wrong instrument type: %s (should be %s)",
				instrumentTransfer.InstrumentType, instrument.InstrumentType)
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsInstrumentNotFound,
				"wrong instrument type", now), "reject")
		}

		logger.Info(instrumentCtx, "Processing transfer")

		// Check if instrument is frozen.
		if isFrozen {
			logger.WarnWithFields(instrumentCtx, []logger.Field{
				logger.Timestamp("now", int64(now)),
			}, "Instrument is frozen")
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsInstrumentFrozen, "", now),
				"reject")
		}

		// Check if instrument is expired or event is over.
		if isExpired {
			logger.WarnWithFields(instrumentCtx, []logger.Field{
				logger.Timestamp("now", int64(now)),
			}, "Instrument is expired")
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsInstrumentNotPermitted, "",
				now), "reject")
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
				logger.Warn(instrumentCtx, "Sender quantity is zero")
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
					"sender quantity is zero", now), "reject")
			}

			senderQuantity += sender.Quantity

			inputOutput, err := transaction.InputOutput(int(sender.Index))
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid sender index : %s", err)
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
					"invalid sender index", now), "reject")
			}

			if !adminLockingScript.Equal(inputOutput.LockingScript) {
				onlyFromAdmin = false
			}

			senderLockingScripts = append(senderLockingScripts, inputOutput.LockingScript)
			allLockingScripts = appendLockingScript(allLockingScripts, inputOutput.LockingScript)
			addBalances = state.AppendZeroBalance(addBalances, inputOutput.LockingScript)
		}

		var receiverLockingScripts []bitcoin.Script
		var receiverQuantity uint64
		for _, receiver := range instrumentTransfer.InstrumentReceivers {
			if receiver.Quantity == 0 {
				logger.Warn(instrumentCtx, "Receiver quantity is zero")
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
					"receiver quantity is zero", now), "reject")
			}

			receiverQuantity += receiver.Quantity

			ra, err := bitcoin.DecodeRawAddress(receiver.Address)
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address : %s", err)
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
					"invalid receiver address", now), "reject")
			}

			lockingScript, err := ra.LockingScript()
			if err != nil {
				logger.Warn(instrumentCtx, "Invalid receiver address script : %s", err)
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
					"invalid receiver address script", now), "reject")
			}

			if !adminLockingScript.Equal(lockingScript) {
				onlyToAdmin = false
			}

			receiverLockingScripts = append(receiverLockingScripts, lockingScript)
			allLockingScripts = appendLockingScript(allLockingScripts, lockingScript)
			addBalances = state.AppendZeroBalance(addBalances, lockingScript)
		}

		if !transfersPermitted && !onlyFromAdmin && !onlyToAdmin {
			logger.Warn(instrumentCtx, "Transfers not permitted")
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsInstrumentNotPermitted, "",
				now), "reject")
		}

		if senderQuantity != receiverQuantity {
			logger.Warn(instrumentCtx,
				"Sender and receiver quantity do not match : sender %d, receiver %d",
				senderQuantity, receiverQuantity)
			return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
				int(instrumentTransfer.ContractIndex), actions.RejectionsMsgMalformed,
				"sender quantity != receiver quantity", now), "reject")
		}

		balances, err := a.balances.AddMulti(instrumentCtx, agentLockingScript, instrumentCode,
			addBalances)
		if err != nil {
			return errors.Wrap(err, "add balances")
		}
		defer a.balances.ReleaseMulti(instrumentCtx, agentLockingScript, instrumentCode, balances)

		balances.Lock()
		defer balances.Unlock()
		defer balances.RevertPending(&txid)

		for i, sender := range instrumentTransfer.InstrumentSenders {
			lockingScript := senderLockingScripts[i]
			balance := balances.Find(lockingScript)
			if balance == nil {
				return fmt.Errorf("Missing balance for sender %d : %s", i, lockingScript)
			}

			if err := balance.AddPendingDebit(sender.Quantity); err != nil {
				logger.WarnWithFields(instrumentCtx, []logger.Field{
					logger.Stringer("locking_script", lockingScript),
					logger.Uint64("quantity", sender.Quantity),
				}, "Failed to add debit : %s", err)

				// TODO This can also be actions.RejectionsHoldingsFrozen. --ce
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, int(sender.Index),
					int(instrumentTransfer.ContractIndex), actions.RejectionsInsufficientQuantity,
					fmt.Sprintf("sender %d: %s", i, err), now), "reject")
			}
		}

		for i, receiver := range instrumentTransfer.InstrumentReceivers {
			lockingScript := receiverLockingScripts[i]
			balance := balances.Find(lockingScript)
			if balance == nil {
				return fmt.Errorf("Missing balance for receiver %d : %s", i, lockingScript)
			}

			if err := balance.AddPendingCredit(receiver.Quantity); err != nil {
				logger.WarnWithFields(instrumentCtx, []logger.Field{
					logger.Stringer("locking_script", lockingScript),
					logger.Uint64("quantity", receiver.Quantity),
				}, "Failed to add credit : %s", err)
				return errors.Wrap(a.sendReject(instrumentCtx, transaction, 0,
					int(instrumentTransfer.ContractIndex), actions.RejectionsInsufficientQuantity,
					err.Error(), now), "reject")
			}
		}

		contractIndex, err := addDustLockingScript(responseTx, agentLockingScript)
		if err != nil {
			return errors.Wrap(err, "add contract locking script")
		}

		transferSettlement := &actions.InstrumentSettlementField{
			ContractIndex:  contractIndex,
			InstrumentType: instrumentType,
			InstrumentCode: instrumentCode[:],
		}

		if err := populateTransferSettlement(responseTx, transferSettlement, balances); err != nil {
			return errors.Wrap(err, "populate settlement")
		}

		settlement.Instruments = append(settlement.Instruments, transferSettlement)

		balances.FinalizePending(&txid, !isSingleContract)

		allBalances = state.AppendBalances(allBalances, balances)

		// TODO Revert balance changes if any part of transfer fails after starting to update
		// balances. --ce
	}

	settlementScript, err := protocol.Serialize(settlement, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize settlement")
	}

	if err := responseTx.AddOutput(settlementScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add settlement output")
	}

	// TODO Verify the contract fee is paid on top of the response tx fee. --ce

	// TODO Verify exchange fee is paid. --ce
	// transfer.ExchangeFee          uint64                     `protobuf:"varint,3,opt,name=ExchangeFee,proto3" json:"ExchangeFee,omitempty"`
	// transfer.ExchangeFeeAddress   []byte
	if transfer.ExchangeFee > 0 {
		ra, err := bitcoin.DecodeRawAddress(transfer.ExchangeFeeAddress)
		if err != nil {
			logger.Warn(ctx, "Invalid exchange fee address : %s", err)
			return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
				actions.RejectionsMsgMalformed, err.Error(), now), "reject")
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.Warn(ctx, "Invalid exchange fee locking script : %s", err)
			return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
				actions.RejectionsMsgMalformed, err.Error(), now), "reject")
		}

		if err := responseTx.AddOutput(lockingScript, transfer.ExchangeFee, false,
			false); err != nil {
			return errors.Wrap(err, "add exchange fee")
		}
	}

	if !isSingleContract {
		// TODO Create a settlement request for the next contract. --ce

		return nil
	}

	if a.ContractFee() > 0 {
		if err := responseTx.AddOutput(a.FeeLockingScript(), a.ContractFee(), true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := responseTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign settlement tx.
	if _, err := responseTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendReject(ctx, transaction, 0, firstContractIndex,
				actions.RejectionsInsufficientTxFeeFunding, err.Error(), now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	settlementTxID := *responseTx.MsgTx.TxHash()
	settlementTransaction, err := a.transactions.AddRaw(ctx, responseTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, settlementTxID)

	// Settle balances regardless of tx acceptance by the network as the agent is the single source
	// of truth.
	allBalances.Settle(txid, settlementTxID, now)

	// Set settlement tx as processed since all the balances were just settled.
	settlementTransaction.Lock()
	settlementTransaction.SetProcessed()
	settlementTransaction.Unlock()

	// Broadcast settlement tx.
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", settlementTxID),
	}, "Responding with settlement")
	if err := a.BroadcastTx(ctx, responseTx.MsgTx); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postSettlementToSubscriptions(ctx, allLockingScripts,
		settlementTransaction); err != nil {
		return errors.Wrap(err, "post settlement")
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
