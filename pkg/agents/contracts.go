package agents

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/pkg/headers"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processContractOffer(ctx context.Context, transaction *transactions.Transaction,
	offer *actions.ContractOffer, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	adminLockingScript := a.AdminLockingScript()

	// First output must be the agent's locking script
	transaction.Lock()
	contractOutput := transaction.Output(0)
	if !agentLockingScript.Equal(contractOutput.LockingScript) {
		transaction.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil, nil // Not for this agent's contract
	}
	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	now := a.Now()

	if contract.Formation != nil {
		return nil, platform.NewRejectError(actions.RejectionsContractExists, "")
	}

	if offer.BodyOfAgreementType == actions.ContractBodyOfAgreementTypeHash &&
		len(offer.BodyOfAgreement) != 32 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"BodyOfAgreement: hash wrong size")
	}

	if offer.ContractExpiration != 0 && offer.ContractExpiration < now {
		return nil, platform.NewRejectError(actions.RejectionsContractExpired, "")
	}

	transaction.Lock()
	txid := transaction.TxID()

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "admin input output")
	}

	isNewAdminLockingScript := false
	if len(adminLockingScript) == 0 {
		adminLockingScript = inputOutput.LockingScript
		isNewAdminLockingScript = true
	} else if !adminLockingScript.Equal(inputOutput.LockingScript) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
			"Not pre-arranged administrative locking script")
	}

	adminAddress, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "admin address")
	}

	var operatorAddress bitcoin.RawAddress
	if offer.ContractOperatorIncluded {
		if transaction.InputCount() < 2 {
			transaction.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"missing operator input")
		}

		inputOutput, err := transaction.InputOutput(1)
		if err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "operator input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "operator address")
		}
		operatorAddress = ra
	}

	transaction.Unlock()

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	// Verify entity contract
	if len(offer.EntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.EntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("EntityContract: %s", err.Error()))
		}
	}

	// Verify operator entity contract
	if len(offer.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.OperatorEntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("OperatorEntityContract: %s", err.Error()))
		}
	}

	if len(offer.MasterAddress) > 0 {
		if _, err := bitcoin.DecodeRawAddress(offer.MasterAddress); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("MasterAddress: %s", err.Error()))
		}
	}

	contractFee := a.ContractFee()
	if offer.ContractFee != contractFee {
		return nil, platform.NewRejectError(actions.RejectionsContractNotPermitted,
			fmt.Sprintf("ContractFee: must be %d, got %d", contractFee, offer.ContractFee))
	}

	if _, err := permissions.PermissionsFromBytes(offer.ContractPermissions,
		len(offer.VotingSystems)); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("ContractPermissions: %s", err.Error()))
	}

	// Validate voting systems are all valid.
	for i, votingSystem := range offer.VotingSystems {
		if err := validateVotingSystem(votingSystem); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("VotingSystems[%d]: %s", i, err.Error()))
		}
	}

	// Check any oracle entity contracts
	for i, oracle := range offer.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Oracles[%d].EntityContract: %s", i, err.Error()))
		}
	}

	// Create contract formation response.
	formation, err := offer.Formation()
	if err != nil {
		return nil, errors.Wrap(err, "formation")
	}

	if err := a.verifyAdminIdentityCertificates(ctx, adminLockingScript, formation,
		now); err != nil {
		return nil, errors.Wrap(err, "admin identity certificates")
	}

	logger.Info(ctx, "Accepting contract offer")

	formation.AdminAddress = adminAddress.Bytes()
	if offer.ContractOperatorIncluded {
		formation.OperatorAddress = operatorAddress.Bytes()
	}
	formation.Timestamp = now
	formation.ContractRevision = 0

	if requestPeerChannel := a.RequestPeerChannel(); requestPeerChannel != nil {
		formation.RequestPeerChannel = requestPeerChannel.String()
	}

	if err := formation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	config := a.Config()
	formationTx := txbuilder.NewTxBuilder(config.FeeRate, config.DustFeeRate)

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	if contractFee > 0 {
		if err := formationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := formationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign formation tx.
	if _, err := formationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	if isNewAdminLockingScript {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
		}, "Updating admin locking script")
		a.SetAdminLockingScript(adminLockingScript)
	}

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the contract is now formed.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, true, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	return etx, nil
}

func (a *Agent) processContractAmendment(ctx context.Context, transaction *transactions.Transaction,
	amendment *actions.ContractAmendment, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	if !amendment.ChangeAdministrationAddress && !amendment.ChangeOperatorAddress &&
		len(amendment.Amendments) == 0 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "missing amendments")
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

	if !bytes.Equal(contract.Formation.AdminAddress, authorizingAddress.Bytes()) &&
		!bytes.Equal(contract.Formation.OperatorAddress, authorizingAddress.Bytes()) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "")
	}

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	if contract.Formation.ContractRevision != amendment.ContractRevision {
		return nil, platform.NewRejectError(actions.RejectionsContractRevision, "")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	config := a.Config()
	vote, err := fetchReferenceVote(ctx, a.caches, a.transactions, agentLockingScript,
		amendment.RefTxID, config.IsTest)
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

		if len(vote.Result.InstrumentCode) != 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("RefTxID: Vote Result: Vote For Instrument: %s", instrumentID))
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(amendment.Amendments) {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment Count: Proposal %d, Amendment %d",
					len(vote.Result.ProposedAmendments), len(amendment.Amendments)))
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(amendment.Amendments[i]) {
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

	if amendment.ChangeAdministrationAddress || amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if len(contract.Formation.OperatorAddress) > 0 {
			// Verify administrator and operator both signed.
			if inputCount < 2 {
				transaction.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
					"missing")
			}

			firstUnlockingScript := transaction.Input(0).UnlockingScript
			firstInputOutput, err := transaction.InputOutput(0)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "first input output")
			}

			secondUnlockingScript := transaction.Input(0).UnlockingScript
			secondInputOutput, err := transaction.InputOutput(1)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "second input output")
			}

			if firstInputOutput.LockingScript.Equal(secondInputOutput.LockingScript) ||
				!contract.IsAdminOrOperator(firstInputOutput.LockingScript) ||
				!contract.IsAdminOrOperator(secondInputOutput.LockingScript) {
				transaction.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsContractBothOperatorsRequired,
					"wrong")
			}

			if isSigHashAll, err := firstUnlockingScript.IsSigHashAll(); err != nil {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll,
					err.Error())
			} else if !isSigHashAll {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
			}

			if isSigHashAll, err := secondUnlockingScript.IsSigHashAll(); err != nil {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll,
					err.Error())
			} else if !isSigHashAll {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
			}
		} else {
			// Verify administrator or operator signed.
			firstUnlockingScript := transaction.Input(0).UnlockingScript
			firstInputOutput, err := transaction.InputOutput(0)
			if err != nil {
				transaction.Unlock()
				return nil, errors.Wrap(err, "first input output")
			}

			if !contract.IsAdminOrOperator(firstInputOutput.LockingScript) {
				transaction.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsNotAdministration, "wrong")
			}

			if isSigHashAll, err := firstUnlockingScript.IsSigHashAll(); err != nil {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
			} else if !isSigHashAll {
				return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
			}
		}
		transaction.Unlock()
	}

	// Copy formation to prevent modification of the original.
	copyScript, err := protocol.Serialize(contract.Formation, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize contract formation")
	}

	action, err := protocol.Deserialize(copyScript, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize contract formation")
	}

	formation, ok := action.(*actions.ContractFormation)
	if !ok {
		return nil, errors.New("ContractFormation script is wrong type")
	}

	// Pull from amendment tx.
	// Administration change. New administration in next input
	inputIndex := 1
	if len(contract.Formation.OperatorAddress) > 0 {
		inputIndex++
	}

	adminAddress, err := bitcoin.DecodeRawAddress(formation.AdminAddress)
	if err != nil {
		return nil, errors.Wrap(err, "admin address")
	}

	isNewAdminLockingScript := false
	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "admin locking script")
	}

	if amendment.ChangeAdministrationAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "missing new admin")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "new admin input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"unsupported new admin")
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_admin_locking_script", adminLockingScript),
			logger.Stringer("new_admin_locking_script", inputOutput.LockingScript),
		}, "Updating admin locking script")
		isNewAdminLockingScript = true
		adminLockingScript = inputOutput.LockingScript
		formation.AdminAddress = ra.Bytes()
		transaction.Unlock()
		inputIndex++
	}

	var operatorLockingScript bitcoin.Script
	if len(formation.OperatorAddress) > 0 {
		operatorAddress, err := bitcoin.DecodeRawAddress(formation.OperatorAddress)
		if err != nil {
			return nil, errors.Wrap(err, "operator address")
		}

		operatorLockingScript, err = operatorAddress.LockingScript()
		if err != nil {
			return nil, errors.Wrap(err, "operator locking script")
		}
	}

	// Operator changes. New operator in second input unless there is also a new administration,
	// then it is in the third input
	if amendment.ChangeOperatorAddress {
		transaction.Lock()
		inputCount := transaction.InputCount()
		if inputIndex >= inputCount {
			transaction.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"missing new operator")
		}

		inputOutput, err := transaction.InputOutput(inputIndex)
		if err != nil {
			transaction.Unlock()
			return nil, errors.Wrap(err, "new operator input output")
		}

		ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript)
		if err != nil {
			transaction.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				"unsupported new operator")
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
		return nil, errors.Wrap(err, "apply amendments")
	}

	// Ensure reduction in instrument quantity limit is not going to put the quantity below the
	// current instrument count.
	if formation.RestrictedQtyInstruments > 0 &&
		formation.RestrictedQtyInstruments < contract.InstrumentCount {
		return nil, platform.NewRejectError(actions.RejectionsContractInstrumentQtyReduction, "")
	}

	// Verify entity contract
	if len(formation.EntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.EntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("EntityContract: %s", err))
		}
	}

	// Verify operator entity contract
	if len(formation.OperatorEntityContract) > 0 {
		if _, err := bitcoin.DecodeRawAddress(formation.OperatorEntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("OperatorEntityContract: %s", err))
		}
	}

	contractFee := a.ContractFee()
	if formation.ContractFee != contractFee {
		return nil, platform.NewRejectError(actions.RejectionsContractNotPermitted,
			fmt.Sprintf("ContractFee: must be %d, got %d", contractFee, formation.ContractFee))
	}

	// Check admin identity oracle signatures
	if err := a.verifyAdminIdentityCertificates(ctx, adminLockingScript, formation,
		now); err != nil {
		return nil, errors.Wrap(err, "apply amendments")
	}

	// Check any oracle entity contracts
	for i, oracle := range formation.Oracles {
		if _, err := bitcoin.DecodeRawAddress(oracle.EntityContract); err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Oracles %d: EntityContract: %s", i, err))
		}
	}

	logger.Info(ctx, "Accepting contract amendment")

	formation.ContractRevision = contract.Formation.ContractRevision + 1 // Bump the revision
	formation.Timestamp = now

	if err := formation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	formationTx := txbuilder.NewTxBuilder(config.FeeRate, config.DustFeeRate)

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	if contractFee > 0 {
		if err := formationTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := formationTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign formation tx.
	if _, err := formationTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	formationTxID := *formationTx.MsgTx.TxHash()

	// Finalize contract formation.
	contract.Formation = formation
	contract.FormationTxID = &formationTxID
	contract.MarkModified()

	if isNewAdminLockingScript {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("admin_locking_script", adminLockingScript),
		}, "Updating admin locking script")
		a.SetAdminLockingScript(adminLockingScript)
	}

	formationTransaction, err := a.transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the contract is now amended.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, true, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	return etx, nil
}

func (a *Agent) processContractFormation(ctx context.Context, transaction *transactions.Transaction,
	formation *actions.ContractFormation, outputIndex int) error {

	// First input must be the agent's locking script
	transaction.Lock()
	input := transaction.Input(0)
	txid := transaction.TxID()
	inputOutput, err := transaction.InputOutput(0)
	transaction.Unlock()
	if err != nil {
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", inputOutput.LockingScript),
		}, "Contract output locking script is wrong")
		return nil // Not for this agent's contract
	}

	if _, err := a.addResponseTxID(ctx, input.PreviousOutPoint.Hash, txid, formation,
		outputIndex); err != nil {
		return errors.Wrap(err, "add response txid")
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

	adminAddress, err := bitcoin.DecodeRawAddress(formation.AdminAddress)
	if err == nil {
		adminLockingScript, err := adminAddress.LockingScript()
		if err == nil {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("admin_locking_script", adminLockingScript),
			}, "Setting admin locking script")
			a.SetAdminLockingScript(adminLockingScript)
		}
	}

	if len(formation.RequestPeerChannel) > 0 {
		requestPeerChannel, err := peer_channels.ParseChannel(formation.RequestPeerChannel)
		if err == nil {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("request_peer_channel", requestPeerChannel),
			}, "Setting request peer channel")
			a.SetRequestPeerChannel(requestPeerChannel)
		}
	}

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

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) processContractAddressChange(ctx context.Context, transaction *transactions.Transaction,
	addressChange *actions.ContractAddressChange,
	outputIndex int) (*expanded_tx.ExpandedTx, error) {

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
		return nil, nil // Not for this agent's contract
	}

	if transaction.OutputCount() < 2 {
		transaction.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"second output must be new contract locking script")
	}
	contractOutput2 := transaction.Output(1)
	newContractLockingScript := contractOutput2.LockingScript

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrapf(err, "input locking script %d", 0)
	}
	authorizingLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	if contract.Formation == nil {
		return nil, platform.NewRejectError(actions.RejectionsContractDoesNotExist, "")
	}

	if contract.MovedTxID != nil {
		return nil, platform.NewRejectError(actions.RejectionsContractMoved,
			contract.MovedTxID.String())
	}

	authorizingAddress, err := bitcoin.RawAddressFromLockingScript(authorizingLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "authorizing address")
	}

	if len(contract.Formation.MasterAddress) == 0 ||
		!bytes.Equal(contract.Formation.MasterAddress, authorizingAddress.Bytes()) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "")
	}

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	newAddress, err := bitcoin.DecodeRawAddress(addressChange.NewContractAddress)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("NewContractAddress: %s", err))
	}

	newLockingScript, err := newAddress.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "new locking script")
	}

	if !newContractLockingScript.Equal(newLockingScript) {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"second output must be new contract locking script")
	}

	newContract, err := a.caches.Contracts.Get(ctx, newLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get new contract")
	}

	if newContract != nil {
		defer a.caches.Contracts.Release(ctx, newLockingScript)
		defer a.caches.Contracts.Save(ctx, newContract)
		newContract.Lock()
		defer newContract.Unlock()

		if newContract.Formation != nil {
			return nil, platform.NewRejectError(actions.RejectionsContractExists,
				"second output must be new contract locking script")
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
		return nil, nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("new_contract_locking_script", newLockingScript),
	}, "Moving contract data to new contract in this system")

	config := a.Config()
	copyScript, err := protocol.Serialize(contract.Formation, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize contract formation")
	}

	action, err := protocol.Deserialize(copyScript, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize contract formation")
	}

	formation, ok := action.(*actions.ContractFormation)
	if !ok {
		return nil, errors.New("ContractFormation script is wrong type")
	}
	newContract.Formation = formation
	newContract.FormationTxID = contract.FormationTxID

	copyBodyScript, err := protocol.Serialize(contract.BodyOfAgreementFormation, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize body of agreement formation")
	}

	bodyAction, err := protocol.Deserialize(copyBodyScript, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize body of agreement formation")
	}

	bodyFormation, ok := bodyAction.(*actions.BodyOfAgreementFormation)
	if !ok {
		return nil, errors.New("BodyOfAgreementFormation script is wrong type")
	}
	newContract.BodyOfAgreementFormation = bodyFormation
	newContract.BodyOfAgreementFormationTxID = contract.BodyOfAgreementFormationTxID

	newContract.InstrumentCount = contract.InstrumentCount
	newContract.FrozenUntil = contract.FrozenUntil

	if newContract.Formation != nil && newContract.FormationTxID != nil {
		if err := a.services.Update(ctx, newLockingScript, newContract.Formation,
			*newContract.FormationTxID); err != nil {
			return nil, errors.Wrap(err, "update services")
		}
	}

	if err := a.caches.Contracts.CopyContractData(ctx, agentLockingScript,
		newLockingScript); err != nil {
		return nil, errors.Wrap(err, "copy contract data")
	}

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil, nil
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

	headers := headers.NewHeadersCache(a.headers)

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

	services, err := a.services.Get(ctx, lockingScript)
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

	a.services.Release(ctx, lockingScript)

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
