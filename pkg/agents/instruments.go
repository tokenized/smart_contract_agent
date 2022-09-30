package agents

import (
	"bytes"
	"context"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processInstrumentCreation(ctx context.Context, transaction *state.Transaction,
	creation *actions.InstrumentCreation) error {

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

	logger.Info(ctx, "Processing instrument creation")

	// Verify instrument payload is valid.
	payload, err := instruments.Deserialize([]byte(creation.InstrumentType),
		creation.InstrumentPayload)
	if err != nil {
		logger.Warn(ctx, "Instrument payload malformed: %s", err)
		return nil
	}

	if err := payload.Validate(); err != nil {
		logger.Warn(ctx, "Instrument payload invalid: %s", err)
		return nil
	}

	a.contract.Lock()

	// Find existing matching instrument
	var existing *state.Instrument
	for _, i := range a.contract.Instruments {
		if bytes.Equal(i.InstrumentCode[:], creation.InstrumentCode) {
			existing = i
			break
		}
	}

	if existing != nil && creation.Timestamp < existing.Creation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(existing.Creation.Timestamp)),
		}, "Older instrument creation")
	}

	previousAuthorizedTokenQty := uint64(0)

	instrumentID, err := protocol.InstrumentIDForRaw(creation.InstrumentType,
		creation.InstrumentCode)
	if err != nil {
		logger.Error(ctx, "Instrument id malformed: %s", err)
	}

	txid := transaction.GetTxID()
	isFirst := existing == nil
	if existing == nil {
		newInstrument := &state.Instrument{
			ContractHash: state.CalculateContractHash(a.contract.LockingScript),
			Creation:     creation,
			CreationTxID: &txid,
		}
		copy(newInstrument.InstrumentType[:], []byte(creation.InstrumentType))
		copy(newInstrument.InstrumentCode[:], creation.InstrumentCode)

		a.contract.Instruments = append(a.contract.Instruments, newInstrument)
	} else {
		if existing.Creation != nil {
			previousAuthorizedTokenQty = existing.Creation.AuthorizedTokenQty
		}
		copy(existing.InstrumentType[:], []byte(creation.InstrumentType))
		copy(existing.InstrumentCode[:], creation.InstrumentCode)
		existing.Creation = creation
		existing.CreationTxID = &txid
		existing.ClearInstrument()
	}

	if err := a.updateAdminBalance(ctx, transaction, instrumentID, creation, txid,
		previousAuthorizedTokenQty); err != nil {
		a.contract.MarkModified()
		a.contract.Unlock()
		return errors.Wrap(err, "admin balance")
	}

	if isFirst {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.String("instrument_id", instrumentID),
		}, "Initial instrument creation")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.String("instrument_id", instrumentID),
		}, "Updated instrument creation")
	}

	a.contract.MarkModified()
	a.contract.Unlock()

	return nil
}

func (a *Agent) updateAdminBalance(ctx context.Context, transaction *state.Transaction,
	instrumentID string, creation *actions.InstrumentCreation, txid bitcoin.Hash32,
	previousAuthorizedTokenQty uint64) error {

	if previousAuthorizedTokenQty == creation.AuthorizedTokenQty {
		return nil // no admin balance update
	}

	if a.contract.Formation == nil {
		return errors.New("Missing contract formation") // no contract formation
	}

	ra, err := bitcoin.DecodeRawAddress(a.contract.Formation.AdminAddress)
	if err != nil {
		return errors.Wrap(err, "admin address")
	}

	adminLockingScript, err := ra.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	contractLockingScript := a.contract.LockingScript
	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)

	balance := &state.Balance{
		LockingScript: adminLockingScript,
		Quantity:      creation.AuthorizedTokenQty,
		Timestamp:     creation.Timestamp,
		TxID:          &txid,
	}

	addedBalance, err := a.balances.Add(ctx, a.contract.LockingScript, instrumentCode, balance)
	if err != nil {
		return errors.Wrap(err, "get balance")
	}
	defer a.balances.Release(ctx, contractLockingScript, instrumentCode, balance)

	var quantity uint64
	if addedBalance != balance {
		addedBalance.Lock()

		if previousAuthorizedTokenQty < creation.AuthorizedTokenQty {
			// Increase
			addedBalance.Quantity += creation.AuthorizedTokenQty - previousAuthorizedTokenQty
		} else { // if previousAuthorizedTokenQty > creation.AuthorizedTokenQty
			// Decrease
			difference := previousAuthorizedTokenQty - creation.AuthorizedTokenQty
			if difference > addedBalance.Quantity {
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.String("instrument_id", instrumentID),
					logger.Uint64("admin_balance", addedBalance.Quantity),
					logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
					logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
				}, "Authorized token quantity reduction more than admin balance")
				addedBalance.Unlock()
				return nil
			}

			addedBalance.Quantity -= difference
		}

		quantity = addedBalance.Quantity

		addedBalance.Timestamp = creation.Timestamp
		addedBalance.TxID = &txid
		addedBalance.MarkModified()
		addedBalance.Unlock()
	}

	logger.ErrorWithFields(ctx, []logger.Field{
		logger.String("instrument_id", instrumentID),
		logger.Uint64("admin_balance", quantity),
		logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
		logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
	}, "Updated authorized token quantity")

	return nil
}
