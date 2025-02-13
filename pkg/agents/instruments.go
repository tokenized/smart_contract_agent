package agents

import (
	"bytes"
	"context"
	"fmt"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processInstrumentDefinition(ctx context.Context, transaction *transactions.Transaction,
	definition *actions.InstrumentDefinition, actionIndex int) (*expanded_tx.ExpandedTx, error) {

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

	// Check if this value wasn't updated properly from a previous bug.
	if contract.InstrumentCount == 0 {
		found := false
		for i := uint64(0); i < uint64(10); i++ {
			nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress, i)
			var instrumentCode state.InstrumentCode
			copy(instrumentCode[:], nextInstrumentCode[:])

			instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
			if err != nil {
				return nil, errors.Wrap(err, "get instrument")
			}

			if instrument != nil {
				contract.InstrumentCount = i + 1
				contract.MarkModified()
				found = true
				a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)
			}
		}

		if found {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Uint64("instrument_count", contract.InstrumentCount),
			}, "Updated instrument count")
		}
	}

	if definition.TransferFee != nil {
		if len(definition.TransferFee.InstrumentCode) > 0 {
			// Check if the administrator is trying to specify the current instrument for the
			// transfer fee to be paid in without using the UseCurrentInstrument field.
			nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
				contract.InstrumentCount)

			if bytes.Equal(nextInstrumentCode[:], definition.TransferFee.InstrumentCode) {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"UseCurrentInstrument field must be used")
			}
		}

		if len(definition.TransferFee.Contract) > 0 ||
			len(definition.TransferFee.InstrumentCode) > 0 {
			return nil, platform.NewRejectError(actions.RejectionsContractNotPermitted,
				"transfer fee to other instrument")
		}
	}

	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++
	contract.MarkModified()

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

	adminBalance, err := a.updateAdminBalance(ctx, agentLockingScript,
		contract.Formation.AdminAddress, creation, txid, actionIndex, 0, &now)
	if err != nil {
		return nil, errors.Wrap(err, "update admin balance")
	}

	if adminBalance != nil {
		defer a.caches.Balances.Release(ctx, agentLockingScript, instrumentCode, adminBalance)
		defer adminBalance.Unlock()

		creation.Timestamp = now // now might have changed while locking balance
	}

	config := a.Config()
	creationTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, config.IsTest)
	if err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "serialize creation")
	}

	creationScriptOutputIndex := len(creationTx.Outputs)
	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFeeOutputIndex := -1
	contractFee := contract.Formation.ContractFee
	if contractFee > 0 {
		contractFeeOutputIndex = len(creationTx.MsgTx.TxOut)
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			if adminBalance != nil {
				adminBalance.Revert(txid, actionIndex)
			}
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if err := a.Sign(ctx, creationTx, a.FeeLockingScript()); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	creationTxID := *creationTx.MsgTx.TxHash()
	creationTransaction, err := a.transactions.AddRaw(ctx, creationTx.MsgTx, nil)
	if err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, creationTxID)

	if adminBalance != nil {
		adminBalance.Settle(ctx, txid, actionIndex, creationTxID, creationScriptOutputIndex, now)
	}

	// Set creation tx as processed since the instrument is now created.
	creationTransaction.Lock()
	creationTransaction.SetProcessed(a.ContractHash(), creationScriptOutputIndex)
	creationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), actionIndex, creationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(creationTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, true, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.updateRequestStats(ctx, &tx, creationTx.MsgTx, actionIndex,
		contractFeeOutputIndex, false, now); err != nil {
		logger.Error(ctx, "Failed to update statistics : %s", err)
	}

	return etx, nil
}

func (a *Agent) processInstrumentModification(ctx context.Context,
	transaction *transactions.Transaction, modification *actions.InstrumentModification,
	actionIndex int) (*expanded_tx.ExpandedTx, error) {

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

	config := a.Config()
	vote, err := fetchReferenceVote(ctx, a.caches, a.transactions, agentLockingScript,
		modification.RefTxID, config.IsTest)
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
	creation := instrument.Creation.Copy()

	if err := applyInstrumentAmendments(creation, contractPermissions, votingSystemsCount,
		modification.Amendments, proposed, proposalType, votingSystem); err != nil {
		return nil, errors.Wrap(err, "apply amendments")
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(agentLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "agent address")
	}

	if creation.TransferFee != nil {
		if len(creation.TransferFee.InstrumentCode) > 0 {
			// Check if the administrator is trying to specify the current instrument for the
			// transfer fee to be paid in without using the UseCurrentInstrument field.
			nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
				contract.InstrumentCount)

			if bytes.Equal(nextInstrumentCode[:], creation.TransferFee.InstrumentCode) {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"UseCurrentInstrument field must be used")
			}
		}

		if len(creation.TransferFee.Contract) > 0 ||
			len(creation.TransferFee.InstrumentCode) > 0 {
			return nil, platform.NewRejectError(actions.RejectionsContractNotPermitted,
				"transfer fee to other instrument")
		}
	}

	logger.Info(ctx, "Accepting instrument modification")

	creation.InstrumentRevision = instrument.Creation.InstrumentRevision + 1 // Bump the revision
	creation.Timestamp = now

	if err := creation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	adminBalance, err := a.updateAdminBalance(ctx, agentLockingScript, adminAddressBytes,
		creation, txid, actionIndex, previousAuthorizedTokenQty, &now)
	if err != nil {
		return nil, errors.Wrap(err, "update admin balance")
	}

	if adminBalance != nil {
		defer a.caches.Balances.Release(ctx, agentLockingScript, instrumentCode, adminBalance)
		defer adminBalance.Unlock()

		creation.Timestamp = now // now might have changed while locking balance
	}

	creationTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	if err := creationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add input")
	}

	if err := creationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add contract output")
	}

	creationScript, err := protocol.Serialize(creation, config.IsTest)
	if err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "serialize creation")
	}

	creationScriptOutputIndex := len(creationTx.Outputs)
	if err := creationTx.AddOutput(creationScript, 0, false, false); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add creation output")
	}

	// Add the contract fee.
	contractFeeOutputIndex := -1
	contractFee := a.ContractFee()
	if contractFee > 0 {
		contractFeeOutputIndex = len(creationTx.MsgTx.TxOut)
		if err := creationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			if adminBalance != nil {
				adminBalance.Revert(txid, actionIndex)
			}
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := creationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "set change")
	}

	// Sign creation tx.
	if err := a.Sign(ctx, creationTx, a.FeeLockingScript()); err != nil {
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
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
		if adminBalance != nil {
			adminBalance.Revert(txid, actionIndex)
		}
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, creationTxID)

	if adminBalance != nil {
		adminBalance.Settle(ctx, txid, actionIndex, creationTxID, creationScriptOutputIndex, now)
	}

	// Set creation tx as processed since the instrument is now modified.
	creationTransaction.Lock()
	creationTransaction.SetProcessed(a.ContractHash(), creationScriptOutputIndex)
	creationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), actionIndex, creationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(creationTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, true, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.updateRequestStats(ctx, &tx, creationTx.MsgTx, actionIndex,
		contractFeeOutputIndex, false, now); err != nil {
		logger.Error(ctx, "Failed to update statistics : %s", err)
	}

	return etx, nil
}

func (a *Agent) processInstrumentCreation(ctx context.Context,
	transaction *transactions.Transaction, creation *actions.InstrumentCreation,
	actionIndex int) error {

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

	requestTransaction, err := a.transactions.Get(ctx, requestTxID)
	if err != nil {
		return errors.Wrapf(err, "get request tx: %s", requestTxID)
	}

	if requestTransaction == nil {
		return errors.Wrapf(err, "missing request tx: %s", requestTxID)
	}

	isTest := a.Config().IsTest
	requestOutputIndex := 0
	requestTransaction.Lock()
	for outputIndex, txout := range requestTransaction.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, isTest)
		if err != nil {
			continue
		}

		switch action.(type) {
		case *actions.InstrumentDefinition, *actions.InstrumentModification:
			requestOutputIndex = outputIndex
		}
	}
	requestTransaction.Unlock()
	a.transactions.Release(ctx, requestTxID)

	if _, err := a.addResponseTxID(ctx, input.PreviousOutPoint.Hash, txid, creation,
		actionIndex); err != nil {
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

	if err := a.applyAdminBalance(ctx, agentLockingScript, adminAddressBytes, creation, requestTxID,
		requestOutputIndex, txid, actionIndex, previousAuthorizedTokenQty,
		creation.Timestamp); err != nil {
		return errors.Wrap(err, "admin balance")
	}

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), actionIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) updateAdminBalance(ctx context.Context, contractLockingScript bitcoin.Script,
	adminAddress []byte, creation *actions.InstrumentCreation, requestTxID bitcoin.Hash32,
	requestActionIndex int, previousAuthorizedTokenQty uint64,
	timestamp *uint64) (*state.Balance, error) {

	if previousAuthorizedTokenQty == creation.AuthorizedTokenQty {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
			logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
		}, "Not updating admin balance")

		return nil, nil // no admin balance update
	}

	ra, err := bitcoin.DecodeRawAddress(adminAddress)
	if err != nil {
		return nil, errors.Wrap(err, "admin address")
	}

	adminLockingScript, err := ra.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "admin locking script")
	}

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)

	addBalance := state.ZeroBalance(adminLockingScript)

	adminBalance, err := a.caches.Balances.Add(ctx, contractLockingScript, instrumentCode,
		addBalance)
	if err != nil {
		return nil, errors.Wrap(err, "get balance")
	}

	// A single balance lock doesn't need to use the balance locker since it isn't
	// susceptible to the group deadlock.
	adminBalance.Lock()

	// Update timestamp to after the balance lock was achieved.
	*timestamp = a.Now()

	if adminBalance != addBalance { // balance was pre-existing
		if previousAuthorizedTokenQty < creation.AuthorizedTokenQty {
			increase := creation.AuthorizedTokenQty - previousAuthorizedTokenQty

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
				logger.Uint64("admin_balance", adminBalance.Quantity),
				logger.Uint64("increase", increase),
				logger.Uint64("new_admin_balance", adminBalance.Quantity+increase),
			}, "Increasing admin balance")

			if err := adminBalance.AddPendingCredit(increase, *timestamp); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("admin_locking_script", adminLockingScript),
					logger.Uint64("increase", increase),
				}, "Failed to add credit : %s", err)

				if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
					rejectError.Message = fmt.Sprintf("admin balance: %s", rejectError.Message)

					return nil, errors.Wrap(rejectError, "add credit")
				}

				return nil, errors.Wrap(err, "add credit")
			}
			adminBalance.SettlePending(requestTxID, requestActionIndex, false)
		} else if previousAuthorizedTokenQty > creation.AuthorizedTokenQty {
			reduction := previousAuthorizedTokenQty - creation.AuthorizedTokenQty

			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
				logger.Uint64("admin_balance", adminBalance.Quantity),
				logger.Uint64("reduction", reduction),
				logger.Uint64("new_admin_balance", adminBalance.Quantity-reduction),
			}, "Decreasing admin balance")

			if err := adminBalance.AddPendingDebit(reduction, *timestamp); err != nil {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("admin_locking_script", adminLockingScript),
					logger.Uint64("reduction", reduction),
				}, "Failed to add debit : %s", err)

				if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
					rejectError.Message = fmt.Sprintf("admin balance: %s", rejectError.Message)

					return nil, errors.Wrap(rejectError, "add debit")
				}

				return nil, errors.Wrap(err, "add debit")
			}
			adminBalance.SettlePending(requestTxID, requestActionIndex, false)
		} else {
			return nil, errors.New("Can't get here because of check at top of function")
		}
	} else if creation.AuthorizedTokenQty == 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
			logger.Uint64("admin_balance", creation.AuthorizedTokenQty),
		}, "New admin balance is zero")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
			logger.Uint64("admin_balance", creation.AuthorizedTokenQty),
		}, "New admin balance")

		if err := adminBalance.AddPendingCredit(creation.AuthorizedTokenQty,
			*timestamp); err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
				logger.Uint64("authorized_quantity", creation.AuthorizedTokenQty),
			}, "Failed to add credit : %s", err)

			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.Message = fmt.Sprintf("admin balance: %s", rejectError.Message)

				return nil, errors.Wrap(rejectError, "add credit")
			}

			return nil, errors.Wrap(err, "add credit")
		}
		adminBalance.SettlePending(requestTxID, requestActionIndex, false)
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
		logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
	}, "Updated authorized token quantity")

	return adminBalance, nil
}

func (a *Agent) applyAdminBalance(ctx context.Context, contractLockingScript bitcoin.Script,
	adminAddress []byte, creation *actions.InstrumentCreation, requestTxID bitcoin.Hash32,
	requestOutputIndex int, responseTxID bitcoin.Hash32, responseOutputIndex int,
	previousAuthorizedTokenQty uint64, timestamp uint64) error {

	if previousAuthorizedTokenQty == creation.AuthorizedTokenQty {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("previous_authorized_quantity", previousAuthorizedTokenQty),
			logger.Uint64("new_authorized_quantity", creation.AuthorizedTokenQty),
		}, "Not updating admin balance")

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

	addBalance := state.ZeroBalance(adminLockingScript)

	adminBalance, err := a.caches.Balances.Add(ctx, contractLockingScript, instrumentCode,
		addBalance)
	if err != nil {
		return errors.Wrap(err, "get balance")
	}
	defer a.caches.Balances.Release(ctx, contractLockingScript, instrumentCode, adminBalance)

	// A single balance lock doesn't need to use the balance locker since it isn't
	// susceptible to the group deadlock.
	adminBalance.Lock()
	defer adminBalance.Unlock()

	// This is processing an instrument creation so it already happened and we are just in
	// recovery mode. We only want to update the admin balance if the provided timestamp is
	// after the last update to the balance.
	if adminBalance.Timestamp > timestamp {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
			logger.Uint64("admin_balance_quantity", adminBalance.Quantity),
			logger.Timestamp("admin_balance_timestamp", int64(adminBalance.Timestamp)),
		}, "Admin balance timestamp more recent than the update action timestamp")
	} else {
		// Set the balance to the current value.
		var newBalance uint64
		if previousAuthorizedTokenQty > creation.AuthorizedTokenQty {
			newBalance -= previousAuthorizedTokenQty - creation.AuthorizedTokenQty
		} else { // previousAuthorizedTokenQty < creation.AuthorizedTokenQty
			newBalance += creation.AuthorizedTokenQty - previousAuthorizedTokenQty
		}

		adminBalance.HardSettle(ctx, requestTxID, requestOutputIndex, responseTxID,
			responseOutputIndex, newBalance, timestamp)
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
			case *instruments.CreditNote:
				if fip[1] == instruments.CreditNoteFieldFaceValue {
					return platform.NewRejectError(actions.RejectionsInstrumentNotPermitted,
						"Amendments on FaceValue prohibited")
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
