package agents

import (
	"bytes"
	"context"
	"fmt"

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

func (a *Agent) processOrder(ctx context.Context, transaction *state.Transaction,
	order *actions.Order, now uint64) error {

	agentLockingScript := a.LockingScript()

	transaction.Lock()

	contractOutput := transaction.Output(0)
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		transaction.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil // Not for this agent's contract
	}

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "admin input output")
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		contract.Unlock()
		return errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) {
		contract.Unlock()
		// TODO Check if the address belongs to an authority oracle. --ce
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, ""), now), "reject")
	}

	if a.contract.Formation == nil {
		contract.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, ""), now), "reject")
	}

	if contract.MovedTxID != nil {
		movedTxID := contract.MovedTxID.String()
		contract.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractMoved, movedTxID), now), "reject")
	}

	// Validate enforcement authority public key and signature
	if len(order.OrderSignature) > 0 || order.SignatureAlgorithm != 0 {
		if order.SignatureAlgorithm != 1 {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "SignatureAlgorithm"), now),
				"reject")
		}

		authorityPubKey, err := bitcoin.PublicKeyFromBytes(order.AuthorityPublicKey)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "AuthorityPublicKey"), now),
				"reject")
		}

		// We want any public key allowed as it could be some jurisdiction that is requiring an
		// enforcement action and not all jurisdiction authorities will be registered authority
		// oracles.

		authoritySig, err := bitcoin.SignatureFromBytes(order.OrderSignature)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "OrderSignature"), now),
				"reject")
		}

		contractAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
		if err != nil {
			return errors.Wrap(err, "contract address")
		}

		sigHash, err := protocol.OrderAuthoritySigHash(ctx, contractAddress, order)
		if err != nil {
			return errors.Wrap(err, "Failed to calculate authority sig hash")
		}

		if !authoritySig.Verify(*sigHash, authorityPubKey) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInvalidSignature, "OrderSignature"), now),
				"reject")
		}
	}

	// Apply logic based on Compliance Action type
	switch order.ComplianceAction {
	case actions.ComplianceActionFreeze:
		return a.processFreezeOrder(ctx, transaction, order, now)
	case actions.ComplianceActionThaw:
		return a.processThawOrder(ctx, transaction, order, now)
	case actions.ComplianceActionConfiscation:
		return a.processConfiscateOrder(ctx, transaction, order, now)
	case "R":
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"ComplianceAction: Reconciliation deprecated, use T3"), now), "reject")
	default:
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "ComplianceAction"), now),
			"reject")
	}

	return nil
}

func (a *Agent) processFreezeOrder(ctx context.Context, transaction *state.Transaction,
	order *actions.Order, now uint64) error {

	logger.Info(ctx, "Processing freeze order")

	agentLockingScript := a.LockingScript()

	transaction.Lock()
	txid := transaction.TxID()
	contractOutput := transaction.Output(0)
	transaction.Unlock()

	contractAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "contract address")
	}

	freeze := &actions.Freeze{
		InstrumentType: order.InstrumentType,
		InstrumentCode: order.InstrumentCode,
		FreezePeriod:   order.FreezePeriod,
		Timestamp:      now,
	}

	freezeTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := freezeTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	isFull := false
	if len(order.TargetAddresses) == 0 {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "TargetAddresses: empty"), now),
			"reject")
	} else if len(order.TargetAddresses) == 1 && bytes.Equal(order.TargetAddresses[0].Address,
		contractAddress.Bytes()) {

		// Contract-Wide action
		isFull = true

		if err := freezeTx.AddOutput(agentLockingScript, 1, false, true); err != nil {
			return errors.Wrap(err, "add contract output")
		}

		freeze.Quantities = append(freeze.Quantities, &actions.QuantityIndexField{
			Index:    0,
			Quantity: 0,
		})
	}

	var contract *state.Contract
	var instrument *state.Instrument
	var lockingScripts []bitcoin.Script
	var quantities []uint64
	var balances state.Balances
	if len(order.InstrumentCode) == 0 {
		if !isFull {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"InstrumentCode: empty in non-full freeze"), now), "reject")
		}

		contract = a.Contract()
		contract.Lock()
		defer contract.Unlock()

		if contract.IsFrozen(now) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsContractFrozen, ""), now), "reject")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "Contract freeze")
	} else {
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], order.InstrumentCode)
		instrumentID, _ := protocol.InstrumentIDForRaw(order.InstrumentType, order.InstrumentCode)

		instrument, err = a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
		if err != nil {
			return errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInstrumentNotFound, ""), now), "reject")
		}
		defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

		instrument.Lock()
		defer instrument.Unlock()

		if instrument.Creation == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInstrumentNotFound, ""), now), "reject")
		}

		if !instrument.Creation.EnforcementOrdersPermitted {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInstrumentNotPermitted, ""), now),
				"reject")
		}

		if isFull {
			if instrument.IsFrozen(now) {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsInstrumentFrozen, ""), now), "reject")
			}

			logger.InfoWithFields(ctx, []logger.Field{
				logger.String("instrument_id", instrumentID),
			}, "Instrument freeze")
		} else {
			used := make(map[bitcoin.Hash20]bool)

			// Validate target addresses
			for i, target := range order.TargetAddresses {
				targetAddress, err := bitcoin.DecodeRawAddress(target.Address)
				if err != nil {
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("TargetAddresses[%d]: Address: %s", i, err)), now),
						"reject")
				}

				if target.Quantity == 0 {
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("TargetAddresses[%d]: Quantity: zero", i)), now), "reject")
				}

				hash, err := targetAddress.Hash()
				if err != nil {
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("TargetAddresses[%d]: Address: Hash: %s", i, err)), now),
						"reject")
				}

				if _, exists := used[*hash]; exists {
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("TargetAddresses[%d]: Address: duplicated", i)), now),
						"reject")
				}

				used[*hash] = true

				// Notify target address
				lockingScript, err := targetAddress.LockingScript()
				if err != nil {
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("TargetAddresses[%d]: Address: Locking Script: %s", i,
								err)), now), "reject")
				}
				lockingScripts = append(lockingScripts, lockingScript)
				quantities = append(quantities, target.Quantity)

				logger.InfoWithFields(ctx, []logger.Field{
					logger.String("instrument_id", instrumentID),
					logger.Stringer("locking_script", lockingScript),
					logger.Uint64("quantity", target.Quantity),
				}, "Freeze target")

				freeze.Quantities = append(freeze.Quantities, &actions.QuantityIndexField{
					Index:    uint32(len(freezeTx.Outputs)),
					Quantity: target.Quantity,
				})

				if err := freezeTx.AddOutput(lockingScript, 1, false, true); err != nil {
					return errors.Wrapf(err, "add target output: %d", i)
				}
			}

			balances, err = a.caches.Balances.GetMulti(ctx, agentLockingScript, instrumentCode,
				lockingScripts)
			if err != nil {
				return errors.Wrap(err, "get balances")
			}
			defer a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

			balances.Lock()
			for i, balance := range balances {
				if balance == nil {
					balances.RevertPending(txid)
					balances.Unlock()
					return errors.Wrap(a.sendRejection(ctx, transaction,
						platform.NewRejectError(actions.RejectionsMsgMalformed,
							fmt.Sprintf("Balance[%d]: not found", i)), now), "reject")
				}
			}
		}
	}

	freezeScript, err := protocol.Serialize(freeze, a.IsTest())
	if err != nil {
		balances.RevertPending(txid)
		balances.Unlock()
		return errors.Wrap(err, "serialize freeze")
	}

	if err := freezeTx.AddOutput(freezeScript, 0, false, false); err != nil {
		balances.RevertPending(txid)
		balances.Unlock()
		return errors.Wrap(err, "add freeze output")
	}

	// Add the contract fee.
	var contractFee uint64
	if contract == nil {
		contractFee = a.ContractFee()
	} else {
		contractFee = contract.Formation.ContractFee
	}
	if contractFee > 0 {
		if err := freezeTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			balances.RevertPending(txid)
			balances.Unlock()
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := freezeTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		balances.RevertPending(txid)
		balances.Unlock()
		return errors.Wrap(err, "set change")
	}

	// Sign freeze tx.
	if _, err := freezeTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			balances.RevertPending(txid)
			balances.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		balances.RevertPending(txid)
		balances.Unlock()
		return errors.Wrap(err, "sign")
	}

	freezeTxID := *freezeTx.MsgTx.TxHash()

	freezeTransaction, err := a.caches.Transactions.AddRaw(ctx, freezeTx.MsgTx, nil)
	if err != nil {
		balances.RevertPending(txid)
		balances.Unlock()
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, freezeTxID)

	balances.Unlock()

	if isFull {
		if len(order.InstrumentCode) == 0 {
			// Mark contract as frozen.
			contract.Freeze(freezeTxID, order.FreezePeriod)
		} else {
			// Mark instrument as frozen.
			instrument.Freeze(freezeTxID, order.FreezePeriod)
		}
	} else {
		for i, balance := range balances {
			balance.AddFreeze(freezeTxID, quantities[i])
		}
	}

	// Set freeze tx as processed.
	freezeTransaction.Lock()
	freezeTransaction.SetProcessed()
	freezeTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(freezeTxID)
	transaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", freezeTxID),
	}, "Responding with freeze")
	if err := a.BroadcastTx(ctx, freezeTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if isFull {
		if err := a.postTransactionToContractSubscriptions(ctx, freezeTransaction); err != nil {
			return errors.Wrap(err, "post freeze to contract")
		}
	} else {
		if err := a.postTransactionToSubscriptions(ctx, lockingScripts,
			freezeTransaction); err != nil {
			return errors.Wrap(err, "post freeze to locking scripts")
		}
	}

	return nil
}

func (a *Agent) processThawOrder(ctx context.Context, transaction *state.Transaction,
	order *actions.Order, now uint64) error {

	logger.Info(ctx, "Processing thaw order")

	agentLockingScript := a.LockingScript()

	transaction.Lock()
	txid := transaction.TxID()
	contractOutput := transaction.Output(0)
	transaction.Unlock()

	thaw := &actions.Thaw{
		FreezeTxId: order.FreezeTxId,
		Timestamp:  now,
	}

	thawTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := thawTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	freezeTxIDHash, err := bitcoin.NewHash32(order.FreezeTxId)
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("FreezeTxId: %s", err)), now), "reject")
	}
	freezeTxID := *freezeTxIDHash

	freezeTransaction, err := a.caches.Transactions.Get(ctx, freezeTxID)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if freezeTransaction == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"FreezeTxId: not found"), now), "reject")
	}
	defer a.caches.Transactions.Release(ctx, freezeTxID)

	freezeTransaction.Lock()

	input := freezeTransaction.Input(0)
	freezeOrderTxID := input.PreviousOutPoint.Hash

	isTest := a.IsTest()
	var freeze *actions.Freeze
	outputCount := freezeTransaction.OutputCount()
	for i := 0; i < outputCount; i++ {
		output := freezeTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Freeze); ok {
			freeze = a
		}
	}

	if freeze == nil {
		freezeTransaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"FreezeTxId: freeze action not found"), now), "reject")
	}

	isFull := false
	if len(freeze.Quantities) == 0 {
		return errors.New("Missing freeze quantities")
	} else if len(freeze.Quantities) == 1 {
		if int(freeze.Quantities[0].Index) >= outputCount {
			freezeTransaction.Unlock()
			return fmt.Errorf("Invalid freeze quantity index %d : %d >= %d", 0,
				freeze.Quantities[0].Index, outputCount)
		}

		firstQuantityOutput := freezeTransaction.Output(int(freeze.Quantities[0].Index))
		if firstQuantityOutput.LockingScript.Equal(agentLockingScript) {
			// Contract-Wide action
			isFull = true

			if err := thawTx.AddOutput(agentLockingScript, 1, false, true); err != nil {
				freezeTransaction.Unlock()
				return errors.Wrap(err, "add contract output")
			}

			freeze.Quantities = append(freeze.Quantities, &actions.QuantityIndexField{
				Index:    0,
				Quantity: 0,
			})
		}
	}

	var contract *state.Contract
	var instrument *state.Instrument
	var lockingScripts []bitcoin.Script
	var balances state.Balances
	if len(freeze.InstrumentCode) == 0 {
		freezeTransaction.Unlock()

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "Contract thaw")

		contract := a.Contract()
		contract.Lock()
		defer contract.Unlock()

		if !contract.IsFrozen(now) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "Contract not frozen"),
				now), "reject")
		}

	} else {
		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], freeze.InstrumentCode)
		instrumentID, _ := protocol.InstrumentIDForRaw(freeze.InstrumentType, freeze.InstrumentCode)

		if isFull {
			freezeTransaction.Unlock()

			logger.InfoWithFields(ctx, []logger.Field{
				logger.String("instrument_id", instrumentID),
			}, "Instrument thaw")

			instrument, err = a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
			if err != nil {
				return errors.Wrap(err, "get instrument")
			}

			if instrument == nil {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsInstrumentNotFound, ""), now),
					"reject")
			}
			defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

			instrument.Lock()
			defer instrument.Unlock()

			if !instrument.IsFrozen(now) {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						"Instrument not frozen"), now), "reject")
			}

		} else {
			for i, target := range freeze.Quantities {
				if int(target.Index) >= outputCount {
					freezeTransaction.Unlock()
					return fmt.Errorf("Invalid freeze quantity index %d : %d >= %d", i,
						target.Index, outputCount)
				}

				output := freezeTransaction.Output(int(target.Index))
				lockingScripts = append(lockingScripts, output.LockingScript)

				if err := thawTx.AddOutput(output.LockingScript, 1, false, true); err != nil {
					freezeTransaction.Unlock()
					return errors.Wrap(err, "add contract output")
				}
			}

			freezeTransaction.Unlock()

			balances, err = a.caches.Balances.GetMulti(ctx, agentLockingScript, instrumentCode,
				lockingScripts)
			if err != nil {
				return errors.Wrap(err, "get balances")
			}
			defer a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

			balances.Lock()
			defer balances.Unlock()
		}
	}

	thawScript, err := protocol.Serialize(thaw, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize thaw")
	}

	if err := thawTx.AddOutput(thawScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add thaw output")
	}

	// Add the contract fee.
	var contractFee uint64
	if contract == nil {
		contractFee = a.ContractFee()
	} else {
		contractFee = contract.Formation.ContractFee
	}
	if contractFee > 0 {
		if err := thawTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := thawTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign thaw tx.
	if _, err := thawTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	thawTxID := *thawTx.MsgTx.TxHash()

	thawTransaction, err := a.caches.Transactions.AddRaw(ctx, thawTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, thawTxID)

	if len(freeze.InstrumentCode) == 0 {
		contract.Thaw()
	} else {
		if isFull {
			instrument.Thaw()
		} else {
			balances.RemoveFreeze(freezeOrderTxID)
		}
	}

	// Set thaw tx as processed.
	thawTransaction.Lock()
	thawTransaction.SetProcessed()
	thawTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(thawTxID)
	transaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", thawTxID),
	}, "Responding with thaw")
	if err := a.BroadcastTx(ctx, thawTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if isFull {
		if err := a.postTransactionToContractSubscriptions(ctx, thawTransaction); err != nil {
			return errors.Wrap(err, "post thaw to contract")
		}
	} else {
		if err := a.postTransactionToSubscriptions(ctx, lockingScripts,
			thawTransaction); err != nil {
			return errors.Wrap(err, "post thaw to locking scripts")
		}
	}

	return nil
}

func (a *Agent) processConfiscateOrder(ctx context.Context, transaction *state.Transaction,
	order *actions.Order, now uint64) error {

	logger.Info(ctx, "Processing confiscation order")

	agentLockingScript := a.LockingScript()

	transaction.Lock()
	txid := transaction.TxID()
	contractOutput := transaction.Output(0)
	transaction.Unlock()

	confiscation := &actions.Confiscation{
		InstrumentType: order.InstrumentType,
		InstrumentCode: order.InstrumentCode,
		Timestamp:      now,
	}

	confiscationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := confiscationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], order.InstrumentCode)
	instrumentID, _ := protocol.InstrumentIDForRaw(order.InstrumentType, order.InstrumentCode)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("instrument_id", instrumentID),
	}, "Instrument ID")

	instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "get instrument")
	}

	if instrument == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsInstrumentNotFound, ""), now), "reject")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()

	if instrument.Creation == nil {
		instrument.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsInstrumentNotFound, ""), now), "reject")
	}

	if !instrument.Creation.EnforcementOrdersPermitted {
		instrument.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsInstrumentNotPermitted, ""), now),
			"reject")
	}

	instrument.Unlock()

	// Validate deposit address, and increase balance by confiscation.DepositQty and increase
	// DepositQty by previous balance
	depositAddress, err := bitcoin.DecodeRawAddress(order.DepositAddress)
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("DepositAddress: %s", err)), now), "reject")
	}

	depositHash, err := depositAddress.Hash()
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("DepositAddress: Hash: %s", err)), now), "reject")
	}

	depositLockingScript, err := depositAddress.LockingScript()
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("DepositAddress: LockingScript: %s", err)), now), "reject")
	}

	hashes := make(map[bitcoin.Hash20]bool)
	depositQuantity := uint64(0)
	var lockingScripts []bitcoin.Script
	var quantities []uint64
	for i, target := range order.TargetAddresses {
		targetAddress, err := bitcoin.DecodeRawAddress(target.Address)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: Address: %s", i, err)), now), "reject")
		}

		if target.Quantity == 0 {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: Quantity: can't be zero", i)), now), "reject")
		}

		hash, err := targetAddress.Hash()
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: Address: Hash: %s", i, err)), now), "reject")
		}

		lockingScript, err := targetAddress.LockingScript()
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: Address: LockingScript: %s", i, err)), now),
				"reject")
		}

		if _, exists := hashes[*hash]; exists {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: Address: duplicated", i)), now), "reject")
		}

		lockingScripts = append(lockingScripts, lockingScript)
		quantities = append(quantities, target.Quantity)

		hashes[*hash] = true
		depositQuantity += target.Quantity

		confiscation.Quantities = append(confiscation.Quantities, &actions.QuantityIndexField{
			Index: uint32(len(confiscationTx.Outputs)),
		})

		if err := confiscationTx.AddOutput(lockingScript, 1, false, true); err != nil {
			return errors.Wrap(err, "add output")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.String("instrument_id", instrumentID),
			logger.Stringer("locking_script", lockingScript),
			logger.Uint64("quantity", target.Quantity),
		}, "Confiscating quantity")
	}

	balances, err := a.caches.Balances.GetMulti(ctx, agentLockingScript, instrumentCode,
		lockingScripts)
	if err != nil {
		return errors.Wrap(err, "get balances")
	}
	defer a.caches.Balances.ReleaseMulti(ctx, agentLockingScript, instrumentCode, balances)

	balances.Lock()
	defer balances.Unlock()

	for i, balance := range balances {
		if balance == nil {
			balances.RevertPending(txid)

			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("TargetAddresses[%d]: no balance", i)), now), "reject")
		}

		quantity := quantities[i]
		finalQuantity, err := balance.AddConfiscation(txid, quantity)
		if err != nil {
			balances.RevertPending(txid)

			logger.WarnWithFields(ctx, []logger.Field{
				logger.String("instrument_id", instrumentID),
				logger.Stringer("locking_script", lockingScripts[i]),
				logger.Uint64("quantity", quantity),
			}, "Failed to add confiscation : %s", err)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.Message = fmt.Sprintf("TargetAddresses[%d]: %s", i, rejectError.Message)
				return errors.Wrap(a.sendRejection(ctx, transaction, rejectError, now), "reject")
			}

			return errors.Wrapf(err, "add confiscation %d", i)
		}

		confiscation.Quantities[i].Quantity = finalQuantity
	}

	if _, exists := hashes[*depositHash]; exists {
		balances.RevertPending(txid)

		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("DepositAddress: duplicated")), now), "reject")
	}

	depositBalance, err := a.caches.Balances.Add(ctx, agentLockingScript, instrumentCode,
		state.ZeroBalance(depositLockingScript))
	if err != nil {
		balances.RevertPending(txid)
		return errors.Wrap(err, "get deposit balance")
	}
	defer a.caches.Balances.Release(ctx, agentLockingScript, instrumentCode, depositBalance)

	if err := depositBalance.AddPendingCredit(depositQuantity); err != nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("locking_script", depositLockingScript),
			logger.Uint64("quantity", depositQuantity),
		}, "Failed to add deposit : %s", err)
		balances.RevertPending(txid)

		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			rejectError.Message = fmt.Sprintf("credit: %s", rejectError.Message)
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError, now), "reject")
		}

		return errors.Wrap(err, "add credit")
	}

	confiscation.DepositQty = depositBalance.SettlePendingQuantity()

	confiscationScript, err := protocol.Serialize(confiscation, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize confiscation")
	}

	if err := confiscationTx.AddOutput(confiscationScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add confiscation output")
	}

	// Add the contract fee.
	contractFee := a.ContractFee()
	if contractFee > 0 {
		if err := confiscationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := confiscationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign confiscation tx.
	if _, err := confiscationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	confiscationTxID := *confiscationTx.MsgTx.TxHash()

	confiscationTransaction, err := a.caches.Transactions.AddRaw(ctx, confiscationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, confiscationTxID)

	balances.FinalizeConfiscation(txid, confiscationTxID, now)
	depositBalance.Settle(txid, confiscationTxID, now)

	// Set confiscation tx as processed.
	confiscationTransaction.Lock()
	confiscationTransaction.SetProcessed()
	confiscationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(confiscationTxID)
	transaction.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("response_txid", confiscationTxID),
	}, "Responding with confiscation")
	if err := a.BroadcastTx(ctx, confiscationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToSubscriptions(ctx, append(lockingScripts, depositLockingScript),
		confiscationTransaction); err != nil {
		return errors.Wrap(err, "post confiscation to locking scripts")
	}

	return nil
}

func (a *Agent) processFreeze(ctx context.Context, transaction *state.Transaction,
	freeze *actions.Freeze, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	return nil
}

func (a *Agent) processThaw(ctx context.Context, transaction *state.Transaction,
	thaw *actions.Thaw, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	return nil
}

func (a *Agent) processConfiscation(ctx context.Context, transaction *state.Transaction,
	confiscation *actions.Confiscation, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	return nil
}

func (a *Agent) processReconciliation(ctx context.Context, transaction *state.Transaction,
	reconciliation *actions.Reconciliation, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	return nil
}
