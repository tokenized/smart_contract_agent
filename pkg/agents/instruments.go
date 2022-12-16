package agents

import (
	"bytes"
	"context"
	"fmt"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processInstrumentDefinition(ctx context.Context, transaction *transactions.Transaction,
	definition *actions.InstrumentDefinition, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()

	transaction.Lock()

	txid := transaction.TxID()

	contractOutput := transaction.Output(0)
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		transaction.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil, nil // Not for this agent's contract
	}

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "admin input output")
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	now := a.Now()

	if err := contract.CheckIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "")
	}

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	if contract.Formation == nil {
		return nil, platform.NewRejectError(actions.RejectionsContractDoesNotExist, "")
	}

	if contract.MovedTxID != nil {
		return nil, platform.NewRejectError(actions.RejectionsContractMoved,
			contract.MovedTxID.String())
	}

	// Verify instrument payload is valid.
	payload, err := instruments.Deserialize([]byte(definition.InstrumentType),
		definition.InstrumentPayload)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("payload invalid: %s", err))
	}

	if err := payload.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("payload invalid: %s", err))
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "agent address")
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
		return nil, errors.Wrap(err, "creation")
	}

	creation.InstrumentCode = instrumentCode[:]
	creation.Timestamp = a.Now()
	creation.InstrumentRevision = 0

	if err := creation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	newInstrument := &state.Instrument{
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode,
		Creation:       creation,
		CreationTxID:   &txid,
	}

	// Add instrument
	instrument, err := a.caches.Instruments.Add(ctx, agentLockingScript, newInstrument)
	if err != nil {
		return nil, errors.Wrap(err, "add instrument")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	if instrument != newInstrument {
		// This should not happen unless the contract's instrument count is wrong and an instrument
		// for the specified index is added more than once.
		return nil, errors.New("Instrument already exists")
	}

	if err := a.updateAdminBalance(ctx, agentLockingScript, contract.Formation.AdminAddress,
		transaction, creation, txid, 0, &now); err != nil {
		return nil, errors.Wrap(err, "update admin balance")
	}
	creation.Timestamp = now // now might have changed while locking balance

	creationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, a.IsTest())
	if err != nil {
		return nil, errors.Wrap(err, "serialize creation")
	}

	creationScriptOutputIndex := len(creationTx.Outputs)
	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
	if contractFee > 0 {
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if _, err := creationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	creationTxID := *creationTx.MsgTx.TxHash()
	creationTransaction, err := a.transactions.AddRaw(ctx, creationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, creationTxID)

	// Set creation tx as processed since the instrument is now created.
	creationTransaction.Lock()
	creationTransaction.SetProcessed(a.ContractHash(), creationScriptOutputIndex)
	creationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, creationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(creationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.Respond(ctx, txid, creationTransaction); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, creationTransaction); err != nil {
		return etx, errors.Wrap(err, "post creation")
	}

	return etx, nil
}

func (a *Agent) processInstrumentModification(ctx context.Context, transaction *transactions.Transaction,
	modification *actions.InstrumentModification,
	outputIndex int) (*expanded_tx.ExpandedTx, error) {

	instrumentID, err := protocol.InstrumentIDForRaw(modification.InstrumentType,
		modification.InstrumentCode)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("InstrumentID: %s", err))
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("instrument_id", instrumentID),
	}, "Instrument ID")

	agentLockingScript := a.LockingScript()

	transaction.Lock()

	txid := transaction.TxID()

	contractOutput := transaction.Output(0)
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		transaction.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil, nil // Not for this agent's contract
	}

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "admin input output")
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	contract.Lock()

	now := a.Now()

	if err := contract.CheckIsAvailable(now); err != nil {
		contract.Unlock()
		return nil, platform.NewDefaultRejectError(err)
	}

	adminAddressBytes := contract.Formation.AdminAddress
	contractPermissions := contract.Formation.ContractPermissions
	votingSystemsCount := len(contract.Formation.VotingSystems)

	contract.Unlock()

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(adminAddressBytes, authorizingAddress.Bytes()) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "")
	}

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	// Get instrument
	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], modification.InstrumentCode)

	instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
	if err != nil {
		return nil, errors.Wrap(err, "get instrument")
	}

	if instrument == nil {
		return nil, platform.NewRejectError(actions.RejectionsInstrumentNotFound, "")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	if instrument.Creation == nil {
		return nil, platform.NewRejectError(actions.RejectionsInstrumentNotFound, "")
	}

	if instrument.Creation.InstrumentRevision != modification.InstrumentRevision {
		return nil, platform.NewRejectError(actions.RejectionsInstrumentRevision, "")
	}

	previousAuthorizedTokenQty := instrument.Creation.AuthorizedTokenQty

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	isTest := a.IsTest()
	vote, err := fetchReferenceVote(ctx, a.caches, a.transactions, agentLockingScript,
		modification.RefTxID, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "fetch vote")
	}

	if vote != nil {
		vote.Lock()

		if len(vote.Result.ProposedAmendments) == 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"RefTxID: Vote Result: Vote Not For Specific Amendments")
		}

		if !bytes.Equal(vote.Result.InstrumentCode, modification.InstrumentCode) {
			vote.Unlock()
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("RefTxID: Vote Result: Vote Not For This Instrument: %s",
					instrumentID))
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(modification.Amendments) {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment Count: Proposal %d, Amendment %d",
					len(vote.Result.ProposedAmendments), len(modification.Amendments)))
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(modification.Amendments[i]) {
				vote.Unlock()
				a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment %d", i))
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
		return nil, errors.Wrap(err, "serialize instrument creation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize instrument creation")
	}

	creation, ok := action.(*actions.InstrumentCreation)
	if !ok {
		return nil, errors.New("InstrumentCreation script is wrong type")
	}

	if err := applyInstrumentAmendments(creation, contractPermissions, votingSystemsCount,
		modification.Amendments, proposed, proposalType, votingSystem); err != nil {
		return nil, errors.Wrap(err, "apply amendments")
	}

	logger.Info(ctx, "Accepting instrument modification")

	creation.InstrumentRevision = instrument.Creation.InstrumentRevision + 1 // Bump the revision
	creation.Timestamp = now

	if err := creation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	if err := a.updateAdminBalance(ctx, agentLockingScript, adminAddressBytes, transaction,
		creation, txid, previousAuthorizedTokenQty, &now); err != nil {
		return nil, errors.Wrap(err, "update admin balance")
	}
	creation.Timestamp = now // now might have changed while locking balance

	creationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize creation")
	}

	creationScriptOutputIndex := len(creationTx.Outputs)
	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFee := a.ContractFee()
	if contractFee > 0 {
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if _, err := creationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	creationTxID := *creationTx.MsgTx.TxHash()

	// Finalize instrument creation.
	instrument.Creation = creation
	instrument.CreationTxID = &creationTxID
	instrument.MarkModified()

	creationTransaction, err := a.transactions.AddRaw(ctx, creationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, creationTxID)

	// Set creation tx as processed since the instrument is now modified.
	creationTransaction.Lock()
	creationTransaction.SetProcessed(a.ContractHash(), creationScriptOutputIndex)
	creationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, creationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(creationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.Respond(ctx, txid, creationTransaction); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, creationTransaction); err != nil {
		return etx, errors.Wrap(err, "post creation")
	}

	return etx, nil
}

func (a *Agent) processInstrumentCreation(ctx context.Context, transaction *transactions.Transaction,
	creation *actions.InstrumentCreation, outputIndex int) error {

	// First input must be the agent's locking script
	transaction.Lock()
	input := transaction.Input(0)
	txid := transaction.TxID()
	requestTxID := transaction.Input(0).PreviousOutPoint.Hash
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	if _, err := a.addResponseTxID(ctx, input.PreviousOutPoint.Hash, txid); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	instrumentID, _ := protocol.InstrumentIDForRaw(creation.InstrumentType, creation.InstrumentCode)

	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("instrument_id", instrumentID),
	}, "Instrument ID")

	var instrumentType [3]byte
	copy(instrumentType[:], []byte(creation.InstrumentType))

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)

	newInstrument := &state.Instrument{
		InstrumentType: instrumentType,
		InstrumentCode: instrumentCode,
		Creation:       creation,
		CreationTxID:   &txid,
	}

	// Find existing matching instrument
	instrument, err := a.caches.Instruments.Add(ctx, agentLockingScript, newInstrument)
	if err != nil {
		return errors.Wrap(err, "add instrument")
	}
	defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

	instrument.Lock()
	defer instrument.Unlock()

	previousAuthorizedTokenQty := uint64(0)

	if instrument == newInstrument {
		// Instrument was created by Add.
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
		}, "Initial instrument creation")

		contract := a.Contract()
		contract.Lock()
		contract.InstrumentCount++
		contract.MarkModified()
		contract.Unlock()
	} else if instrument.Creation == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
		}, "Replacing empty instrument creation")

		instrument.Creation = creation
		instrument.CreationTxID = &txid
		instrument.MarkModified()
	} else if creation.Timestamp < instrument.Creation.Timestamp {
		// Newer version of instrument already existed.
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(instrument.Creation.Timestamp)),
		}, "Older instrument creation")
		return nil
	} else if creation.Timestamp == instrument.Creation.Timestamp {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
		}, "Already processed instrument creation")
		return nil
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(creation.Timestamp)),
			logger.Timestamp("previous_timestamp", int64(instrument.Creation.Timestamp)),
		}, "Updating instrument creation")
		previousAuthorizedTokenQty = instrument.Creation.AuthorizedTokenQty

		instrument.Creation = creation
		instrument.CreationTxID = &txid
		instrument.MarkModified()
	}

	var adminAddressBytes []byte
	contract := a.Contract()
	contract.Lock()
	if contract.Formation != nil {
		adminAddressBytes = contract.Formation.AdminAddress
	}
	contract.Unlock()

	if len(adminAddressBytes) == 0 {
		// Get admin address from request transaction
		adminLockingScript, err := getRequestFirstInputLockingScript(ctx, a.transactions,
			requestTxID)
		if err != nil {
			return errors.Wrap(err, "get request tx locking script")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(adminLockingScript)
		if err != nil {
			return errors.Wrap(err, "admin address")
		}

		adminAddressBytes = ra.Bytes()
	}

	now := a.Now()
	if err := a.updateAdminBalance(ctx, agentLockingScript, adminAddressBytes, transaction,
		creation, txid, previousAuthorizedTokenQty, &now); err != nil {
		return errors.Wrap(err, "admin balance")
	}
	creation.Timestamp = now // now might have changed while locking balance

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) updateAdminBalance(ctx context.Context, contractLockingScript bitcoin.Script,
	adminAddress []byte, transaction *transactions.Transaction, creation *actions.InstrumentCreation,
	txid bitcoin.Hash32, previousAuthorizedTokenQty uint64, now *uint64) error {

	if previousAuthorizedTokenQty == creation.AuthorizedTokenQty {
		return nil // no admin balance update
	}

	ra, err := bitcoin.DecodeRawAddress(adminAddress)
	if err != nil {
		return errors.Wrap(err, "admin address")
	}

	adminLockingScript, err := ra.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)

	balance := &state.Balance{
		LockingScript: adminLockingScript,
		Quantity:      creation.AuthorizedTokenQty,
		Timestamp:     creation.Timestamp,
		TxID:          &txid,
	}

	addedBalance, err := a.caches.Balances.Add(ctx, contractLockingScript, instrumentCode, balance)
	if err != nil {
		return errors.Wrap(err, "get balance")
	}
	defer a.caches.Balances.Release(ctx, contractLockingScript, instrumentCode, balance)

	if addedBalance != balance {
		// A single balance lock doesn't need to use the balance locker since it isn't
		// susceptible to the group deadlock.
		addedBalance.Lock()
		defer addedBalance.Unlock()

		if previousAuthorizedTokenQty < creation.AuthorizedTokenQty {
			increase := creation.AuthorizedTokenQty - previousAuthorizedTokenQty

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
				logger.Uint64("admin_balance", addedBalance.Quantity),
				logger.Uint64("increase", increase),
			}, "Increasing admin balance")

			addedBalance.Quantity += increase
		} else { // if previousAuthorizedTokenQty > creation.AuthorizedTokenQty
			reduction := previousAuthorizedTokenQty - creation.AuthorizedTokenQty
			if reduction > addedBalance.Quantity {
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.Stringer("admin_locking_script", adminLockingScript),
					logger.Uint64("admin_balance", addedBalance.Quantity),
					logger.Uint64("reduction", reduction),
					logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
					logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
				}, "Authorized token quantity reduction more than admin balance")
				return nil
			}

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
				logger.Uint64("admin_balance", addedBalance.Quantity),
				logger.Uint64("reduction", reduction),
			}, "Decreasing admin balance")

			addedBalance.Quantity -= reduction
		}

		addedBalance.Timestamp = *now
		addedBalance.TxID = &txid
		addedBalance.MarkModified()
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
			logger.Uint64("admin_balance", creation.AuthorizedTokenQty),
		}, "New admin balance")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
		logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
	}, "Updated authorized token quantity")

	return nil
}

// applyInstrumentAmendments applies the amendments to the instrument creation.
func applyInstrumentAmendments(instrumentCreation *actions.InstrumentCreation,
	permissionBytes []byte, votingSystemsCount int, amendments []*actions.AmendmentField,
	proposed bool, proposalType, votingSystem uint32) error {

	var instrumentPayload instruments.Instrument
	perms, err := permissions.PermissionsFromBytes(permissionBytes, votingSystemsCount)
	if err != nil {
		return fmt.Errorf("Invalid contract permissions : %s", err)
	}

	for i, amendment := range amendments {
		fip, err := permissions.FieldIndexPathFromBytes(amendment.FieldIndexPath)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: FieldIndexPath: %s", i, err))
		}
		if len(fip) == 0 {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: missing field index", i))
		}
		applied := false
		var fieldPermissions permissions.Permissions

		switch fip[0] {
		case actions.InstrumentFieldInstrumentType:
			return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
				"Instrument type amendments prohibited")

		case actions.InstrumentFieldInstrumentPermissions:
			if _, err := permissions.PermissionsFromBytes(amendment.Data,
				votingSystemsCount); err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: InstrumentPermissions amendment value is invalid : %s",
						i, err))
			}

		case actions.InstrumentFieldInstrumentPayload:
			if len(fip) == 1 {
				return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
					"Amendments on complex fields (InstrumentPayload) prohibited")
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
					fmt.Sprintf("Amendments %d: apply: %s", i, err))
			}
			if len(fieldPermissions) == 0 {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: permissions invalid", i))
			}

			switch instrumentPayload.(type) {
			case *instruments.Membership:
				if fip[1] == instruments.MembershipFieldMembershipClass {
					return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
						"Amendments on MembershipClass prohibited")
				}
			}

			applied = true // Amendment already applied
		}

		if !applied {
			fieldPermissions, err = instrumentCreation.ApplyAmendment(fip, amendment.Operation,
				amendment.Data, perms)
			if err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: apply: %s", i, err))
			}
			if len(fieldPermissions) == 0 {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: permissions invalid", i))
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
							i, fip))
				}
			case 1: // Holder
				if !permission.HolderProposal {
					return platform.NewRejectError(actions.RejectionsContractPermissions,
						fmt.Sprintf("Amendments %d: Field %s: not permitted by holder proposal",
							i, fip))
				}
			case 2: // Administrative Matter
				if !permission.AdministrativeMatter {
					return platform.NewRejectError(actions.RejectionsContractPermissions,
						fmt.Sprintf("Amendments %d: Field %s: not permitted by administrative vote",
							i, fip))
				}
			default:
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: invalid proposal initiator type: %d", i,
						proposalType))
			}

			if int(votingSystem) >= len(permission.VotingSystemsAllowed) {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: voting system out of range: %d", i, votingSystem))
			}
			if !permission.VotingSystemsAllowed[votingSystem] {
				return platform.NewRejectError(actions.RejectionsContractPermissions,
					fmt.Sprintf("Amendments %d: Field %s: not allowed using voting system %d",
						i, fip, votingSystem))
			}
		} else if !permission.Permitted {
			return platform.NewRejectError(actions.RejectionsContractPermissions,
				fmt.Sprintf("Amendments %d: Field %s: not permitted without proposal", i, fip))
		}
	}

	if instrumentPayload != nil {
		if err = instrumentPayload.Validate(); err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Instrument Payload invalid after amendments: %s", err))
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
			fmt.Sprintf("Instrument invalid after amendments: %s", err))
	}

	return nil
}
