package agents

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processContractOffer(ctx context.Context, transaction *state.Transaction,
	offer *actions.ContractOffer, outputIndex int, now uint64) error {

	agentLockingScript := a.LockingScript()

	// First output must be the agent's locking script
	transaction.Lock()
	contractOutput := transaction.Output(0)
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		transaction.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil // Not for this agent's contract
	}
	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	if contract.Formation != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractExists, ""), now), "reject")
	}

	if offer.BodyOfAgreementType == actions.ContractBodyOfAgreementTypeHash &&
		len(offer.BodyOfAgreement) != 32 {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"BodyOfAgreement: hash wrong size"), now), "reject")
	}

	if offer.ContractExpiration != 0 && offer.ContractExpiration < now {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractExpired, ""), now), "reject")
	}

	transaction.Lock()
	txid := transaction.TxID()

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "admin input output")
	}

	adminLockingScript := inputOutput.LockingScript
	adminAddress, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "admin address")
	}

	var operatorAddress bitcoin.RawAddress
	if offer.ContractOperatorIncluded {
		if transaction.InputCount() < 2 {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing operator input"),
				now), "reject")
		}

		inputOutput, err := transaction.InputOutput(1)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "operator input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "operator address")
		}
		operatorAddress = ra
	}

	transaction.Unlock()

	// Verify entity contract
	if len(offer.EntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("EntityContract: %s", err.Error())), now), "reject")
		}
	}

	// Verify operator entity contract
	if len(offer.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.OperatorEntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("OperatorEntityContract: %s", err.Error())), now), "reject")
		}
	}

	if len(offer.MasterAddress) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.MasterAddress); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("MasterAddress: %s", err.Error())), now), "reject")
		}
	}

	if _, err := permissions.PermissionsFromBytes(offer.ContractPermissions,
		len(offer.VotingSystems)); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("ContractPermissions: %s", err.Error())), now), "reject")
	}

	// Validate voting systems are all valid.
	for i, votingSystem := range offer.VotingSystems {
		if err := validateVotingSystem(votingSystem); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("VotingSystems[%d]: %s", i, err.Error())), now), "reject")
		}
	}

	// Check any oracle entity contracts
	for i, oracle := range offer.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Oracles[%d].EntityContract: %s", i, err.Error())), now), "reject")
		}
	}

	// Create contract formation response.
	formation, err := offer.Formation()
	if err != nil {
		return errors.Wrap(err, "formation")
	}

	if err := a.verifyAdminIdentityCertificates(ctx, adminLockingScript, formation,
		now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "admin identity certificates")
	}

	logger.Info(ctx, "Accepting contract offer")

	formation.AdminAddress = adminAddress.Bytes()
	if offer.ContractOperatorIncluded {
		formation.OperatorAddress = operatorAddress.Bytes()
	}
	formation.Timestamp = now
	formation.ContractRevision = 0

	formationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	contractFee := offer.ContractFee
	if contractFee > 0 {
		if err := formationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := formationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign formation tx.
	if _, err := formationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the contract is now formed.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return errors.Wrap(err, "expanded tx")
	}

	if err := a.BroadcastTx(ctx, etx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.Respond(ctx, txid, formationTransaction); err != nil {
		return errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return errors.Wrap(err, "post formation")
	}

	return nil
}

func (a *Agent) processContractAmendment(ctx context.Context, transaction *state.Transaction,
	amendment *actions.ContractAmendment, outputIndex int, now uint64) error {

	if !amendment.ChangeAdministrationAddress && !amendment.ChangeOperatorAddress &&
		len(amendment.Amendments) == 0 {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "missing amendments"), now),
			"reject")
	}

	agentLockingScript := a.LockingScript()

	// First output must be the agent's locking script
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

	if err := contract.CheckIsAvailable(now); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, err, now), "reject")
	}

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) &&
		!bytes.Equal(contract.Formation.OperatorAddress, authorizingAddress.Bytes()) {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, ""), now), "reject")
	}

	if contract.Formation.ContractRevision != amendment.ContractRevision {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractRevision, ""), now), "reject")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	vote, err := fetchReferenceVote(ctx, a.caches, agentLockingScript, amendment.RefTxID,
		a.IsTest())
	if err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "fetch vote")
	}

	if vote != nil {
		vote.Lock()

		if len(vote.Result.ProposedAmendments) == 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result: Vote Not For Specific Amendments"), now), "reject")
		}

		if len(vote.Result.InstrumentCode) != 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote For Instrument: %s", instrumentID)),
				now), "reject")
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(amendment.Amendments) {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment Count: Proposal %d, Amendment %d",
						len(vote.Result.ProposedAmendments), len(amendment.Amendments))), now),
				"reject")
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(amendment.Amendments[i]) {
				vote.Unlock()
				a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment %d", i)), now),
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

	if amendment.ChangeAdministrationAddress || amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if len(contract.Formation.OperatorAddress) > 0 {
			// Verify administrator and operator both signed.
			if inputCount < 2 {
				transaction.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
						"missing"), now), "reject")
			}

			firstInputOutput, err := transaction.InputOutput(0)
			if err != nil {
				transaction.Unlock()
				return errors.Wrap(err, "first input output")
			}

			secondInputOutput, err := transaction.InputOutput(1)
			if err != nil {
				transaction.Unlock()
				return errors.Wrap(err, "second input output")
			}

			if firstInputOutput.LockingScript.Equal(secondInputOutput.LockingScript) ||
				!contract.IsAdminOrOperator(firstInputOutput.LockingScript) ||
				!contract.IsAdminOrOperator(secondInputOutput.LockingScript) {
				transaction.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
						"wrong"), now), "reject")
			}
		} else {
			// Verify administrator or operator signed.
			firstInputOutput, err := transaction.InputOutput(0)
			if err != nil {
				transaction.Unlock()
				return errors.Wrap(err, "first input output")
			}

			if !contract.IsAdminOrOperator(firstInputOutput.LockingScript) {
				transaction.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
					platform.NewRejectError(actions.RejectionsNotAdministration, "wrong"), now),
					"reject")
			}
		}
		transaction.Unlock()
	}

	// Copy formation to prevent modification of the original.
	isTest := a.IsTest()
	copyScript, err := protocol.Serialize(contract.Formation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize contract formation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return errors.Wrap(err, "deserialize contract formation")
	}

	formation, ok := action.(*actions.ContractFormation)
	if !ok {
		return errors.New("ContractFormation script is wrong type")
	}

	// Pull from amendment tx.
	// Administration change. New administration in next input
	inputIndex := 1
	if len(contract.Formation.OperatorAddress) > 0 {
		inputIndex++
	}

	adminAddress, err := bitcoin.DecodeRawAddress(formation.AdminAddress)
	if err != nil {
		return errors.Wrap(err, "admin address")
	}

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	if amendment.ChangeAdministrationAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing new admin"), now),
				"reject")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "new admin input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "unsupported new admin"),
				now), "reject")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_admin_locking_script", adminLockingScript),
			logger.Stringer("new_admin_locking_script", inputOutput.LockingScript),
		}, "Updating admin locking script")
		adminLockingScript = inputOutput.LockingScript
		formation.AdminAddress = ra.Bytes()
		transaction.Unlock()
		inputIndex++
	}

	var operatorLockingScript bitcoin.Script
	if len(formation.OperatorAddress) > 0 {
		operatorAddress, err := bitcoin.DecodeRawAddress(formation.OperatorAddress)
		if err != nil {
			return errors.Wrap(err, "operator address")
		}

		operatorLockingScript, err = operatorAddress.LockingScript()
		if err != nil {
			return errors.Wrap(err, "operator locking script")
		}
	}

	// Operator changes. New operator in second input unless there is also a new administration,
	// then it is in the third input
	if amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing new operator"),
				now), "reject")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "new operator input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "unsupported new operator"),
				now), "reject")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_operator_locking_script", operatorLockingScript),
			logger.Stringer("new_operator_locking_script", inputOutput.LockingScript),
		}, "Updating operator locking script")
		operatorLockingScript = inputOutput.LockingScript
		formation.OperatorAddress = ra.Bytes()
		transaction.Unlock()
		inputIndex++
	}

	if err := applyContractAmendments(formation, amendment.Amendments, proposed, proposalType,
		votingSystem); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	// Ensure reduction in instrument quantity limit is not going to put the quantity below the
	// current instrument count.
	if formation.RestrictedQtyInstruments > 0 &&
		formation.RestrictedQtyInstruments < contract.InstrumentCount {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractInstrumentQtyReduction, ""), now),
			"reject")
	}

	// Verify entity contract
	if len(formation.EntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("EntityContract: %s", err)), now), "reject")
		}
	}

	// Verify operator entity contract
	if len(formation.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.OperatorEntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("OperatorEntityContract: %s", err)), now), "reject")
		}
	}

	// Check admin identity oracle signatures
	if err := a.verifyAdminIdentityCertificates(ctx, adminLockingScript, formation,
		now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex, rejectError, now),
				"reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	// Check any oracle entity contracts
	for i, oracle := range formation.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Oracles %d: EntityContract: %s", i, err)), now), "reject")
		}
	}

	logger.Info(ctx, "Accepting contract amendment")

	formation.ContractRevision = contract.Formation.ContractRevision + 1 // Bump the revision
	formation.Timestamp = now

	formationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
	if contractFee > 0 {
		if err := formationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := formationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign formation tx.
	if _, err := formationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error()),
				now), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	formationTxID := *formationTx.MsgTx.TxHash()

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = &formationTxID
	contract.MarkModified()

	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the contract is now amended.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return errors.Wrap(err, "expanded tx")
	}

	if err := a.BroadcastTx(ctx, etx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.Respond(ctx, txid, formationTransaction); err != nil {
		return errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return errors.Wrap(err, "post formation")
	}

	return nil
}

func (a *Agent) processContractFormation(ctx context.Context, transaction *state.Transaction,
	formation *actions.ContractFormation, outputIndex int, now uint64) error {

	// First input must be the agent's locking script
	transaction.Lock()
	input := transaction.Input(0)
	txid := transaction.TxID()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	if _, err := a.addResponseTxID(ctx, input.PreviousOutPoint.Hash, outputIndex,
		txid); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", inputOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil // Not for this agent's contract
	}

	defer a.caches.Contracts.Save(ctx, a.contract)
	a.contract.Lock()
	defer a.contract.Unlock()

	isFirst := a.contract.Formation == nil

	previousTimeStamp := int64(0)
	if a.contract.Formation != nil {
		previousTimeStamp = int64(a.contract.Formation.Timestamp)
		if formation.Timestamp < a.contract.Formation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
			}, "Older contract formation")
			return nil
		} else if formation.Timestamp == a.contract.Formation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
			}, "Already processed contract formation")
			return nil
		}
	}

	a.contract.Formation = formation
	a.contract.FormationTxID = &txid
	a.contract.MarkModified()

	if isFirst {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Initial contract formation")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("previous_timestamp", previousTimeStamp),
		}, "Updating contract formation")
	}

	return nil
}

func (a *Agent) processContractAddressChange(ctx context.Context, transaction *state.Transaction,
	addressChange *actions.ContractAddressChange, outputIndex int, now uint64) error {

	logger.Info(ctx, "Processing contract address change")

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

	if transaction.OutputCount() < 2 {
		transaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"second output must be new contract locking script"), now), "reject")
	}
	contractOutput2 := transaction.Output(1)
	newContractLockingScript := contractOutput2.LockingScript

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	if contract.Formation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, ""), now), "reject")
	}

	if contract.MovedTxID != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsContractMoved, contract.MovedTxID.String()),
			now), "reject")
	}

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return errors.Wrap(err, "authorizing address")
	}

	if len(contract.Formation.MasterAddress) == 0 ||
		!bytes.Equal(contract.Formation.MasterAddress, authorizingAddress.Bytes()) {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, ""), now), "reject")
	}

	newAddress, err := bitcoin.DecodeRawAddress(addressChange.NewContractAddress)
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("NewContractAddress: %s", err)), now), "reject")
	}

	newLockingScript, err := newAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "new locking script")
	}

	if !newContractLockingScript.Equal(newLockingScript) {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"second output must be new contract locking script"), now), "reject")
	}

	newContract, err := a.caches.Contracts.Get(ctx, newLockingScript)
	if err != nil {
		return errors.Wrap(err, "get new contract")
	}

	if newContract != nil {
		defer a.caches.Contracts.Release(ctx, newLockingScript)
		defer a.caches.Contracts.Save(ctx, newContract)
		newContract.Lock()
		defer newContract.Unlock()

		if newContract.Formation != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsContractExists,
					"second output must be new contract locking script"), now), "reject")
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("new_contract_locking_script", newLockingScript),
	}, "Accepting contract address change")

	contract.MovedTxID = &txid

	if newContract == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("new_contract_locking_script", newLockingScript),
		}, "New contract not found in this system")
		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("new_contract_locking_script", newLockingScript),
	}, "Moving contract data to new contract in this system")

	isTest := a.IsTest()
	copyScript, err := protocol.Serialize(contract.Formation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize contract formation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return errors.Wrap(err, "deserialize contract formation")
	}

	formation, ok := action.(*actions.ContractFormation)
	if !ok {
		return errors.New("ContractFormation script is wrong type")
	}
	newContract.Formation = formation
	newContract.FormationTxID = contract.FormationTxID

	copyBodyScript, err := protocol.Serialize(contract.BodyOfAgreementFormation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize body of agreement formation")
	}

	bodyAction, err := protocol.Deserialize(copyBodyScript, isTest)
	if err != nil {
		return errors.Wrap(err, "deserialize body of agreement formation")
	}

	bodyFormation, ok := bodyAction.(*actions.BodyOfAgreementFormation)
	if !ok {
		return errors.New("BodyOfAgreementFormation script is wrong type")
	}
	newContract.BodyOfAgreementFormation = bodyFormation
	newContract.BodyOfAgreementFormationTxID = contract.BodyOfAgreementFormationTxID

	newContract.InstrumentCount = contract.InstrumentCount
	newContract.FrozenUntil = contract.FrozenUntil

	if newContract.Formation != nil && newContract.FormationTxID != nil {
		if err := a.caches.Services.Update(ctx, newLockingScript, newContract.Formation,
			*newContract.FormationTxID); err != nil {
			return errors.Wrap(err, "update services")
		}
	}

	if err := a.caches.Contracts.CopyContractData(ctx, agentLockingScript,
		newLockingScript); err != nil {
		return errors.Wrap(err, "copy contract data")
	}

	return nil
}

func (a *Agent) verifyAdminIdentityCertificates(ctx context.Context,
	adminLockingScript bitcoin.Script, contractFormation *actions.ContractFormation,
	now uint64) error {

	for i, certificate := range contractFormation.AdminIdentityCertificates {
		if err := a.verifyAdminIdentityCertificate(ctx, adminLockingScript, contractFormation,
			certificate, now); err != nil {
			if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
				rejectError.Message = fmt.Sprintf("AdminIdentityCertificates[%d]: %s", i,
					rejectError.Message)
				return err
			}

			return errors.Wrapf(err, "certificate %d", i)
		}
	}

	return nil
}

func (a *Agent) verifyAdminIdentityCertificate(ctx context.Context,
	adminLockingScript bitcoin.Script, contractFormation *actions.ContractFormation,
	certificate *actions.AdminIdentityCertificateField, now uint64) error {

	headers := platform.NewHeadersCache(a.headers)

	if certificate.Expiration != 0 && certificate.Expiration < now {
		return platform.NewRejectError(actions.RejectionsInvalidSignature, "expired")
	}

	ra, err := bitcoin.DecodeRawAddress(certificate.EntityContract)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("EntityContract: %s", err))
	}

	lockingScript, err := ra.LockingScript()
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("EntityContract: locking script: %s", err))
	}

	services, err := a.caches.Services.Get(ctx, lockingScript)
	if err != nil {
		return errors.Wrap(err, "get service")
	}

	if services == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "Service Not Found")
	}

	var publicKey *bitcoin.PublicKey
	for _, service := range services.Services {
		if service.Type != actions.ServiceTypeIdentityOracle {
			continue
		}

		publicKey = &service.PublicKey
		break
	}

	a.caches.Services.Release(ctx, lockingScript)

	if publicKey == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "Identity Service Not Found")
	}

	signature, err := bitcoin.SignatureFromBytes(certificate.Signature)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, fmt.Sprintf("Signature: %s",
			err))
	}

	// Check if block time is beyond expiration
	expire := now + uint64((time.Hour * 6).Nanoseconds())
	header, err := headers.GetHeader(ctx, int(certificate.BlockHeight))
	if err != nil {
		return errors.Wrap(err, "get header")
	}
	if header == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "Header Height Not Found")
	}

	if header.Timestamp > uint32(expire) {
		return fmt.Errorf("Oracle sig block hash expired : %d < %d", header.Timestamp, expire)
	}

	hash := header.BlockHash()

	logFields := []logger.Field{}

	logFields = append(logFields, logger.Stringer("admin_locking_script", adminLockingScript))

	var entity interface{}
	if len(contractFormation.EntityContract) > 0 {
		// Use parent entity contract address in signature instead of entity structure.
		entityRA, err := bitcoin.DecodeRawAddress(contractFormation.EntityContract)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("EntityContract: %s", err))
		}

		entity = entityRA

		lockingScript, err := entityRA.LockingScript()
		if err == nil {
			logFields = append(logFields, logger.Stringer("entity_contract_locking_script",
				lockingScript))
		}
	} else {
		entity = contractFormation.Issuer

		logFields = append(logFields, logger.JSON("issuer", *contractFormation.Issuer))
	}

	logFields = append(logFields, logger.Stringer("block_hash", hash))
	logFields = append(logFields, logger.Uint64("expiration", certificate.Expiration))

	adminAddress, err := bitcoin.RawAddressFromLockingScript(adminLockingScript)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Admin locking script: %s", err))
	}

	sigHash, err := protocol.ContractAdminIdentityOracleSigHash(ctx, adminAddress, entity, *hash,
		certificate.Expiration, 1)
	if err != nil {
		return errors.Wrap(err, "sig hash")
	}

	logFields = append(logFields, logger.Hex("sig_hash", sigHash[:]))

	if !signature.Verify(*sigHash, *publicKey) {
		logger.WarnWithFields(ctx, logFields, "Admin identity certificate is invalid")
		return platform.NewRejectError(actions.RejectionsInvalidSignature, "invalid signature")
	}

	logger.InfoWithFields(ctx, logFields, "Admin identity certificate is valid")
	return nil
}

// applyContractAmendments applies the amendments to the contract formation.
func applyContractAmendments(contractFormation *actions.ContractFormation,
	amendments []*actions.AmendmentField, proposed bool, proposalType, votingSystem uint32) error {

	perms, err := permissions.PermissionsFromBytes(contractFormation.ContractPermissions,
		len(contractFormation.VotingSystems))
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

		switch fip[0] {
		case actions.ContractFieldContractPermissions:
			if _, err := permissions.PermissionsFromBytes(amendment.Data,
				len(contractFormation.VotingSystems)); err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: Data: %s", i, err))
			}
		}

		fieldPermissions, err := contractFormation.ApplyAmendment(fip, amendment.Operation,
			amendment.Data, perms)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: apply: %s", i, err))
		}
		if len(fieldPermissions) == 0 {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: permissions invalid", i))
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

	// Check validity of updated contract data
	if err := contractFormation.Validate(); err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Contract invalid after amendments: %s", err))
	}

	return nil
}

// postTransactionToContractSubscriptions posts the transaction to any subscriptions for the
// relevant locking scripts for the contract.
func (a *Agent) postTransactionToContractSubscriptions(ctx context.Context,
	transaction *state.Transaction) error {

	// agentLockingScript := a.LockingScript()

	// subscriptions, err := a.caches.Subscriptions.GetLockingScriptMulti(ctx, agentLockingScript,
	// 	lockingScripts)
	// if err != nil {
	// 	return errors.Wrap(err, "get subscriptions")
	// }
	// defer a.caches.Subscriptions.ReleaseMulti(ctx, agentLockingScript, subscriptions)

	// if len(subscriptions) == 0 {
	// 	return nil
	// }

	// expandedTx, err := transaction.ExpandedTx(ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "get expanded tx")
	// }

	// msg := channels_expanded_tx.ExpandedTxMessage(*expandedTx)

	// for _, subscription := range subscriptions {
	// 	if subscription == nil {
	// 		continue
	// 	}

	// 	subscription.Lock()
	// 	channelHash := subscription.GetChannelHash()
	// 	subscription.Unlock()

	// 	// Send settlement over channel
	// 	channel, err := a.GetChannel(ctx, channelHash)
	// 	if err != nil {
	// 		return errors.Wrapf(err, "get channel : %s", channelHash)
	// 	}
	// 	if channel == nil {
	// 		continue
	// 	}

	// 	if err := channel.SendMessage(ctx, &msg); err != nil {
	// 		logger.WarnWithFields(ctx, []logger.Field{
	// 			logger.Stringer("channel", channelHash),
	// 		}, "Failed to send channels message : %s", err)
	// 	}
	// }

	return nil
}
