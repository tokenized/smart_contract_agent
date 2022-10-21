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
	offer *actions.ContractOffer, now uint64) error {

	// First output must be the agent's locking script
	transaction.Lock()
	contractOutput := transaction.Output(0)
	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing contract offer")

	contract := a.Contract()

	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	if contract.Formation != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractExists, "", now)), "reject")
	}

	if offer.BodyOfAgreementType == 1 && len(offer.BodyOfAgreement) != 32 {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"BodyOfAgreement: hash wrong size", now)), "reject")
	}

	if offer.ContractExpiration != 0 && offer.ContractExpiration < now {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractExpired, "", now)), "reject")
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
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing operator input",
					now)), "reject")
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
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("EntityContract: %s", err.Error()), now)), "reject")
		}
	}

	// Verify operator entity contract
	if len(offer.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.OperatorEntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("OperatorEntityContract: %s", err.Error()), now)), "reject")
		}
	}

	if len(offer.MasterAddress) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.MasterAddress); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("MasterAddress: %s", err.Error()), now)), "reject")
		}
	}

	if _, err := permissions.PermissionsFromBytes(offer.ContractPermissions,
		len(offer.VotingSystems)); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("ContractPermissions: %s", err.Error()), now)), "reject")
	}

	// Validate voting systems are all valid.
	for i, votingSystem := range offer.VotingSystems {
		if err := validateVotingSystem(votingSystem); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("VotingSystems[%d]: %s", i, err.Error()), now)), "reject")
		}
	}

	// Check any oracle entity contracts
	for i, oracle := range offer.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Oracles[%d].EntityContract: %s", i, err.Error()), now)), "reject")
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
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "admin identity certificates")
	}

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
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	logger.Info(ctx, "Accepted contract offer")

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	if err := a.BroadcastTx(ctx, formationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	// TODO Post to any contract subscribers. Maybe post to all subscribers for anything to this
	// contract. Like if they are subscribed for a locking script under this contract, then send
	// this transaction. --ce

	return nil
}

func (a *Agent) processContractAmendment(ctx context.Context, transaction *state.Transaction,
	amendment *actions.ContractAmendment, now uint64) error {

	// First output must be the agent's locking script
	transaction.Lock()

	txid := transaction.TxID()
	contractOutput := transaction.Output(0)

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "admin input output")
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		return nil // Not for this agent's contract
	}

	logger.Info(ctx, "Processing contract amendment")

	contract := a.Contract()

	contract.Lock()
	defer contract.Unlock()

	if contract.Formation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "authorizing address")
	}

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) &&
		!bytes.Equal(contract.Formation.OperatorAddress, authorizingAddress.Bytes()) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "", now)), "reject")
	}

	if contract.Formation.ContractRevision != amendment.ContractRevision {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractRevision, "", now)), "reject")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	if len(amendment.RefTxID) != 0 { // Vote Result Action allowing these amendments
		proposed = true

		refTxID, err := bitcoin.NewHash32(amendment.RefTxID)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "RefTxID", now)), "reject")
		}

		// Retrieve Vote Result
		voteResultTransaction, err := a.caches.Transactions.Get(ctx, *refTxID)
		if err != nil {
			return errors.Wrap(err, "get ref tx")
		}

		if voteResultTransaction == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result Tx Not Found", now)), "reject")
		}
		defer a.caches.Transactions.Release(ctx, *refTxID)

		var voteResult *actions.Result
		isTest := a.IsTest()
		voteResultTransaction.Lock()
		outputCount := voteResultTransaction.OutputCount()
		for i := 0; i < outputCount; i++ {
			output := voteResultTransaction.Output(i)
			action, err := protocol.Deserialize(output.LockingScript, isTest)
			if err != nil {
				continue
			}

			if r, ok := action.(*actions.Result); ok {
				voteResult = r
			}
		}
		voteResultTransaction.Unlock()

		if voteResult == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result Not Found", now)), "reject")
		}

		// Retrieve Vote
		voteTxID, err := bitcoin.NewHash32(voteResult.VoteTxId)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: VoteTxId: %s", err), now)), "reject")
		}

		// Retrieve the vote
		vote, err := a.caches.Votes.Get(ctx, *voteTxID)
		if err != nil {
			return errors.Wrap(err, "get vote data")
		}

		if vote == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsVoteNotFound,
					"RefTxID: Vote Result: Vote Data Not Found", now)), "reject")
		}
		defer a.caches.Votes.Release(ctx, *voteTxID)

		if vote.Proposal == nil || vote.Result == nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result: Vote Not Completed", now)), "reject")
		}

		if vote.Result.Result != "A" {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote Not Accepted: %s", vote.Result.Result),
					now)), "reject")
		}

		if len(vote.Result.ProposedAmendments) == 0 {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					"RefTxID: Vote Result: Vote Not For Specific Amendments", now)), "reject")
		}

		if len(vote.Result.InstrumentCode) != 0 {
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote For Instrument: %s", instrumentID),
					now)), "reject")
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(amendment.Amendments) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote For Different Amendment Count: Proposal %d, Amendment %d",
						len(vote.Result.ProposedAmendments), len(amendment.Amendments)), now)), "reject")
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(amendment.Amendments[i]) {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						fmt.Sprintf("RefTxID: Vote Result: Vote For Different Amendment %d", i),
						now)), "reject")
			}
		}

		proposalType = vote.Proposal.Type
		votingSystem = vote.Proposal.VoteSystem
	}

	if amendment.ChangeAdministrationAddress || amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if len(contract.Formation.OperatorAddress) > 0 {
			if inputCount < 2 {
				transaction.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
						"missing", now)), "reject")
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
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
						"wrong", now)), "reject")
			}
		} else {
			firstInputOutput, err := transaction.InputOutput(0)
			if err != nil {
				transaction.Unlock()
				return errors.Wrap(err, "first input output")
			}

			if !contract.IsAdminOrOperator(firstInputOutput.LockingScript) {
				transaction.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsNotAdministration, "wrong", now)),
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
	// Administration change. New administration in second input
	inputIndex := 1
	if len(contract.Formation.OperatorAddress) > 0 {
		inputIndex++
	}

	var adminLockingScript bitcoin.Script
	if amendment.ChangeAdministrationAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing new admin", now)),
				"reject")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "new admin input output")
		}

		adminLockingScript = inputOutput.LockingScript
		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "unsupported new admin",
					now)), "reject")
		}

		formation.AdminAddress = ra.Bytes()
		transaction.Unlock()
		inputIndex++
	}

	if len(adminLockingScript) == 0 {
		adminAddress, err := bitcoin.DecodeRawAddress(formation.AdminAddress)
		if err != nil {
			return errors.Wrap(err, "admin address")
		}

		adminLockingScript, err = adminAddress.LockingScript()
		if err != nil {
			return errors.Wrap(err, "admin locking script")
		}
	}

	// Operator changes. New operator in second input unless there is also a new administration,
	// then it is in the third input
	if amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "missing new operator",
					now)), "reject")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "new operator input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed, "unsupported new operator",
					now)), "reject")
		}

		formation.OperatorAddress = ra.Bytes()
		transaction.Unlock()
		inputIndex++
	}

	if err := applyContractAmendments(formation, amendment.Amendments, proposed, proposalType,
		votingSystem, now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	// Ensure reduction in instrument quantity limit is not going to put the quantity below the
	// current instrument count.
	if formation.RestrictedQtyInstruments > 0 &&
		formation.RestrictedQtyInstruments < contract.InstrumentCount {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractInstrumentQtyReduction, "", now)),
			"reject")
	}

	// Verify entity contract
	if len(formation.EntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("EntityContract: %s", err), now)), "reject")
		}
	}

	// Verify operator entity contract
	if len(formation.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.OperatorEntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("OperatorEntityContract: %s", err), now)), "reject")
		}
	}

	// Check admin identity oracle signatures
	if err := a.verifyAdminIdentityCertificates(ctx, adminLockingScript, formation,
		now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	// Check any oracle entity contracts
	for i, oracle := range formation.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Oracles %d: EntityContract: %s", i, err), now)), "reject")
		}
	}

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
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	logger.Info(ctx, "Accepted contract amendment")

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	if err := a.BroadcastTx(ctx, formationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	// TODO Post to any contract subscribers. Maybe post to all subscribers for anything to this
	// contract. Like if they are subscribed for a locking script under this contract, then send
	// this transaction. --ce

	return nil
}

func (a *Agent) processContractFormation(ctx context.Context, transaction *state.Transaction,
	formation *actions.ContractFormation, now uint64) error {

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

	logger.Info(ctx, "Processing contract formation")

	a.contract.Lock()

	isFirst := a.contract.Formation == nil

	if a.contract.Formation != nil && formation.Timestamp < a.contract.Formation.Timestamp {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
			logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
		}, "Older contract formation")
	}

	a.contract.Formation = formation
	txid := transaction.GetTxID()
	a.contract.FormationTxID = &txid
	a.contract.MarkModified()

	if isFirst {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Initial contract formation")
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Updated contract formation")
	}

	a.contract.Unlock()

	return nil
}

func validateVotingSystem(system *actions.VotingSystemField) error {
	if system.VoteType != "R" && system.VoteType != "A" && system.VoteType != "P" {
		return fmt.Errorf("Unsupported vote type : %s", system.VoteType)
	}
	if system.ThresholdPercentage == 0 || system.ThresholdPercentage >= 100 {
		return fmt.Errorf("Threshold Percentage out of range : %d", system.ThresholdPercentage)
	}
	if system.TallyLogic != 0 && system.TallyLogic != 1 {
		return fmt.Errorf("Tally Logic invalid : %d", system.TallyLogic)
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
		return platform.NewRejectError(actions.RejectionsInvalidSignature, "expired", now)
	}

	ra, err := bitcoin.DecodeRawAddress(certificate.EntityContract)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("EntityContract: %s", err), now)
	}

	lockingScript, err := ra.LockingScript()
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("EntityContract: locking script: %s", err), now)
	}

	services, err := a.caches.Services.Get(ctx, lockingScript)
	if err != nil {
		return errors.Wrap(err, "get service")
	}

	if services == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			"Service Not Found", now)
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
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "Identity Service Not Found",
			now)
	}

	signature, err := bitcoin.SignatureFromBytes(certificate.Signature)
	if err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Signature: %s", err), now)
	}

	// Check if block time is beyond expiration
	expire := now + uint64((time.Hour * 6).Nanoseconds())
	header, err := headers.GetHeader(ctx, int(certificate.BlockHeight))
	if err != nil {
		return errors.Wrap(err, "get header")
	}
	if header == nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed, "Header Height Not Found",
			now)
	}

	if header.Timestamp > uint32(expire) {
		return fmt.Errorf("Oracle sig block hash expired : %d < %d",
			header.Timestamp, expire)
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
				fmt.Sprintf("EntityContract: %s", err), now)
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
			fmt.Sprintf("Admin locking script: %s", err), now)
	}

	sigHash, err := protocol.ContractAdminIdentityOracleSigHash(ctx, adminAddress, entity, *hash,
		certificate.Expiration, 1)
	if err != nil {
		return errors.Wrap(err, "sig hash")
	}

	logFields = append(logFields, logger.Hex("sig_hash", sigHash[:]))

	if !signature.Verify(*sigHash, *publicKey) {
		logger.WarnWithFields(ctx, logFields, "Admin identity certificate is invalid")
		return platform.NewRejectError(actions.RejectionsInvalidSignature, "invalid signature", now)
	}

	logger.InfoWithFields(ctx, logFields, "Admin identity certificate is valid")
	return nil
}

// applyContractAmendments applies the amendments to the contract formation.
func applyContractAmendments(contractFormation *actions.ContractFormation,
	amendments []*actions.AmendmentField, proposed bool, proposalType, votingSystem uint32,
	now uint64) error {

	perms, err := permissions.PermissionsFromBytes(contractFormation.ContractPermissions,
		len(contractFormation.VotingSystems))
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

		switch fip[0] {
		case actions.ContractFieldContractPermissions:
			if _, err := permissions.PermissionsFromBytes(amendment.Data,
				len(contractFormation.VotingSystems)); err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: Data: %s", i, err), now)
			}
		}

		fieldPermissions, err := contractFormation.ApplyAmendment(fip, amendment.Operation,
			amendment.Data, perms)
		if err != nil {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: apply: %s", i, err), now)
		}
		if len(fieldPermissions) == 0 {
			return platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Amendments %d: permissions invalid", i), now)
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

	// Check validity of updated contract data
	if err := contractFormation.Validate(); err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Formation invalid after amendments: %s", err), now)
	}

	return nil
}
