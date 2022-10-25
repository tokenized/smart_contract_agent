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
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processInstrumentDefinition(ctx context.Context, transaction *state.Transaction,
	definition *actions.InstrumentDefinition, now uint64) error {

	logger.Info(ctx, "Processing instrument definition")

	agentLockingScript := a.LockingScript()

	transaction.Lock()

	txid := transaction.TxID()

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
	defer contract.Unlock()

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "", now)), "reject")
	}

	if a.contract.Formation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	// Verify instrument payload is valid.
	payload, err := instruments.Deserialize([]byte(definition.InstrumentType),
		definition.InstrumentPayload)
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("payload invalid: %s", err), now)), "reject")
	}

	if err := payload.Validate(); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("payload invalid: %s", err), now)), "reject")
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "agent address")
	}

	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++

	instrumentID, _ := protocol.InstrumentIDForRaw(definition.InstrumentType, nextInstrumentCode[:])
	ctx = logger.ContextWithLogFields(ctx, logger.String("instrument_id", instrumentID))

	logger.Info(ctx, "Accepting instrument definition")

	// Create instrument creation response.
	var instrumentType [3]byte
	copy(instrumentType[:], []byte(definition.InstrumentType))

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], nextInstrumentCode[:])

	creation, err := definition.Creation()
	if err != nil {
		return errors.Wrap(err, "creation")
	}

	creation.Timestamp = now
	creation.InstrumentRevision = 0

	newInstrument := &state.Instrument{
		ContractHash:   state.CalculateContractHash(agentLockingScript),
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode,
		Creation:       creation,
		CreationTxID:   &txid,
	}

	// Add instrument
	instrument, err := a.caches.Instruments.Add(ctx, newInstrument)
	if err != nil {
		return errors.Wrap(err, "add instrument")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	if instrument != newInstrument {
		// This should not happen unless the contract's instrument count is wrong and an instrument
		// for the specified index is added more than once.
		return errors.New("Instrument already exists")
	}

	creationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize creation")
	}

	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
	if contractFee > 0 {
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if _, err := creationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	creationTxID := *creationTx.MsgTx.TxHash()
	creationTransaction, err := a.caches.Transactions.AddRaw(ctx, creationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, creationTxID)

	// Set creation tx as processed since the instrument is now created.
	creationTransaction.Lock()
	creationTransaction.SetProcessed()
	creationTransaction.Unlock()

	if err := a.BroadcastTx(ctx, creationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, creationTransaction); err != nil {
		return errors.Wrap(err, "post creation")
	}

	return nil
}

func (a *Agent) processInstrumentModification(ctx context.Context, transaction *state.Transaction,
	modification *actions.InstrumentModification, now uint64) error {

	instrumentID, _ := protocol.InstrumentIDForRaw(modification.InstrumentType,
		modification.InstrumentCode)
	ctx = logger.ContextWithLogFields(ctx, logger.String("instrument_id", instrumentID))

	logger.Info(ctx, "Processing instrument modification")

	agentLockingScript := a.LockingScript()

	transaction.Lock()

	txid := transaction.TxID()

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
	contract.Lock()
	if a.contract.Formation == nil {
		contract.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	adminAddressBytes := contract.Formation.AdminAddress
	contractPermissions := contract.Formation.ContractPermissions
	votingSystemsCount := len(contract.Formation.VotingSystems)
	contract.Unlock()

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(adminAddressBytes, authorizingAddress.Bytes()) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "", now)), "reject")
	}

	// Get instrument
	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], modification.InstrumentCode)

	instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
	if err != nil {
		return errors.Wrap(err, "get instrument")
	}

	if instrument == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsInstrumentNotFound, "", now)), "reject")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	if instrument.Creation.InstrumentRevision != modification.InstrumentRevision {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsInstrumentRevision, "", now)), "reject")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	isTest := a.IsTest()
	vote, err := fetchReferenceVote(ctx, a.caches, agentLockingScript, modification.RefTxID,
		isTest, now)
	if err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "fetch vote")
	}

	if vote != nil {
		vote.Lock()

		if len(vote.Result.ProposedAmendments) == 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result: Vote Not For Specific Amendments", now)), "reject")
		}

		if !bytes.Equal(vote.Result.InstrumentCode, modification.InstrumentCode) {
			vote.Unlock()
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote Not For This Instrument: %s",
						instrumentID), now)), "reject")
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(modification.Amendments) {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment Count: Proposal %d, Amendment %d",
						len(vote.Result.ProposedAmendments), len(modification.Amendments)), now)),
				"reject")
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(modification.Amendments[i]) {
				vote.Unlock()
				a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment %d", i), now)),
					"reject")
			}
		}

		voteTxID := *vote.VoteTxID
		proposed = true
		proposalType = vote.Proposal.Type
		votingSystem = vote.Proposal.VoteSystem

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("vote_txid", voteTxID),
		}, "Verified amendments from vote")

		vote.Unlock()
		a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)
	}

	// Copy creation to prevent modification of the original.
	copyScript, err := protocol.Serialize(instrument.Creation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize instrument creation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return errors.Wrap(err, "deserialize instrument creation")
	}

	creation, ok := action.(*actions.InstrumentCreation)
	if !ok {
		return errors.New("InstrumentCreation script is wrong type")
	}

	if err := applyInstrumentAmendments(creation, contractPermissions, votingSystemsCount,
		modification.Amendments, proposed, proposalType, votingSystem, now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	logger.Info(ctx, "Accepting instrument modification")

	creation.InstrumentRevision = instrument.Creation.InstrumentRevision + 1 // Bump the revision
	creation.Timestamp = now

	creationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize creation")
	}

	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
	if contractFee > 0 {
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if _, err := creationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	creationTxID := *creationTx.MsgTx.TxHash()

	// Finalize instrument creation.
	instrument.Creation = creation
	instrument.CreationTxID = &creationTxID
	instrument.MarkModified()

	creationTransaction, err := a.caches.Transactions.AddRaw(ctx, creationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, creationTxID)

	// Set creation tx as processed since the instrument is now modified.
	creationTransaction.Lock()
	creationTransaction.SetProcessed()
	creationTransaction.Unlock()

	if err := a.BroadcastTx(ctx, creationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, creationTransaction); err != nil {
		return errors.Wrap(err, "post creation")
	}

	return nil
}

func (a *Agent) processInstrumentCreation(ctx context.Context, transaction *state.Transaction,
	creation *actions.InstrumentCreation, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	txid := transaction.TxID()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	instrumentID, _ := protocol.InstrumentIDForRaw(creation.InstrumentType, creation.InstrumentCode)
	ctx = logger.ContextWithLogFields(ctx, logger.String("instrument_id", instrumentID))

	logger.Info(ctx, "Processing instrument creation")

	var instrumentType [3]byte
	copy(instrumentType[:], []byte(creation.InstrumentType))

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)

	newInstrument := &state.Instrument{
		ContractHash:   state.CalculateContractHash(agentLockingScript),
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode,
		Creation:       creation,
		CreationTxID:   &txid,
	}

	a.contract.Lock()
	defer a.contract.Unlock()

	// Find existing matching instrument
	instrument, err := a.caches.Instruments.Add(ctx, newInstrument)
	if err != nil {
		return errors.Wrap(err, "add instrument")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	previousAuthorizedTokenQty := uint64(0)

	if instrument == newInstrument {
		// Instrument was created by Add.
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
		}, "Initial instrument creation")
	} else if creation.Timestamp < instrument.Creation.Timestamp {
		// Instrument already existed.
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(instrument.Creation.Timestamp)),
		}, "Older instrument creation")
		return nil
	} else if creation.Timestamp < instrument.Creation.Timestamp {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
		}, "Already processed instrument creation")
		return nil
	} else {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("previous_timestamp", int64(instrument.Creation.Timestamp)),
		}, "Updating instrument creation")
		previousAuthorizedTokenQty = instrument.Creation.AuthorizedTokenQty

		instrument.Creation = creation
		instrument.CreationTxID = &txid
	}

	if err := a.updateAdminBalance(ctx, transaction, creation, txid,
		previousAuthorizedTokenQty); err != nil {
		return errors.Wrap(err, "admin balance")
	}

	return nil
}

func (a *Agent) updateAdminBalance(ctx context.Context, transaction *state.Transaction,
	creation *actions.InstrumentCreation, txid bitcoin.Hash32,
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

	addedBalance, err := a.caches.Balances.Add(ctx, a.contract.LockingScript, instrumentCode,
		balance)
	if err != nil {
		return errors.Wrap(err, "get balance")
	}
	defer a.caches.Balances.Release(ctx, contractLockingScript, instrumentCode, balance)

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
		logger.Uint64("admin_balance", quantity),
		logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
		logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
	}, "Updated authorized token quantity")

	return nil
}

// applyInstrumentAmendments applies the amendments to the instrument creation.
func applyInstrumentAmendments(instrumentCreation *actions.InstrumentCreation,
	permissionBytes []byte, votingSystemsCount int, amendments []*actions.AmendmentField,
	proposed bool, proposalType, votingSystem uint32, now uint64) error {

	var instrumentPayload instruments.Instrument
	perms, err := permissions.PermissionsFromBytes(permissionBytes, votingSystemsCount)
	if err != nil {
		return fmt.Errorf("Invalid contract permissions : %s", err)
	}

	for i, amendment := range amendments {
		fip, err := permissions.FieldIndexPathFromBytes(amendment.FieldIndexPath)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: FieldIndexPath: %s", i, err), now)
		}
		if len(fip) == 0 {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: missing field index", i), now)
		}
		applied := false
		var fieldPermissions permissions.Permissions

		switch fip[0] {
		case actions.InstrumentFieldInstrumentType:
			return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
				"Instrument type amendments prohibited", now)

		case actions.InstrumentFieldInstrumentPermissions:
			if _, err := permissions.PermissionsFromBytes(amendment.Data,
				votingSystemsCount); err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: InstrumentPermissions amendment value is invalid : %s",
						i, err), now)
			}

		case actions.InstrumentFieldInstrumentPayload:
			if len(fip) == 1 {
				return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
					"Amendments on complex fields (InstrumentPayload) prohibited", now)
			}

			if instrumentPayload == nil {
				// Get payload object
				instrumentPayload, err = instruments.Deserialize([]byte(instrumentCreation.InstrumentType),
					instrumentCreation.InstrumentPayload)
				if err != nil {
					errors.Wrap(err, "payload deserialize")
				}
			}

			payloadPermissions, err := perms.SubPermissions(
				permissions.FieldIndexPath{actions.InstrumentFieldInstrumentPayload}, 0, false)

			fieldPermissions, err = instrumentPayload.ApplyAmendment(fip[1:], amendment.Operation,
				amendment.Data, payloadPermissions)
			if err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: apply: %s", i, err), now)
			}
			if len(fieldPermissions) == 0 {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: permissions invalid", i), now)
			}

			switch instrumentPayload.(type) {
			case *instruments.Membership:
				if fip[1] == instruments.MembershipFieldMembershipClass {
					return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
						"Amendments on MembershipClass prohibited", now)
				}
			}

			applied = true // Amendment already applied
		}

		if !applied {
			fieldPermissions, err = instrumentCreation.ApplyAmendment(fip, amendment.Operation,
				amendment.Data, perms)
			if err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: apply: %s", i, err), now)
			}
			if len(fieldPermissions) == 0 {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: permissions invalid", i), now)
			}
		}

		// fieldPermissions are the permissions that apply to the field that was changed in the
		// amendment.
		permission := fieldPermissions[0]
		if proposed {
			switch proposalType {
			case 0: // Administration
				if !permission.AdministrationProposal {
					return platform.NewRejectError(actions.RejectionsContractPermissions,
						fmt.Sprintf("Amendments %d: Field %s: not permitted by administration proposal",
							i, fip), now)
				}
			case 1: // Holder
				if !permission.HolderProposal {
					return platform.NewRejectError(actions.RejectionsContractPermissions,
						fmt.Sprintf("Amendments %d: Field %s: not permitted by holder proposal",
							i, fip), now)
				}
			case 2: // Administrative Matter
				if !permission.AdministrativeMatter {
					return platform.NewRejectError(actions.RejectionsContractPermissions,
						fmt.Sprintf("Amendments %d: Field %s: not permitted by administrative vote",
							i, fip), now)
				}
			default:
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: invalid proposal initiator type: %d", i,
						proposalType), now)
			}

			if int(votingSystem) >= len(permission.VotingSystemsAllowed) {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: voting system out of range: %d", i, votingSystem),
					now)
			}
			if !permission.VotingSystemsAllowed[votingSystem] {
				return platform.NewRejectError(actions.RejectionsContractPermissions,
					fmt.Sprintf("Amendments %d: Field %s: not allowed using voting system %d",
						i, fip, votingSystem), now)
			}
		} else if !permission.Permitted {
			return platform.NewRejectError(actions.RejectionsContractPermissions,
				fmt.Sprintf("Amendments %d: Field %s: not permitted without proposal", i, fip), now)
		}
	}

	if instrumentPayload != nil {
		if err = instrumentPayload.Validate(); err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Instrument Payload invalid after amendments: %s", err), now)
		}

		newPayload, err := instrumentPayload.Bytes()
		if err != nil {
			return errors.Wrap(err, "serialize payload")
		}

		instrumentCreation.InstrumentPayload = newPayload
	}

	// Check validity of updated contract data
	if err := instrumentCreation.Validate(); err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Instrument invalid after amendments: %s", err), now)
	}

	return nil
}
