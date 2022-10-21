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
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processBodyOfAgreementOffer(ctx context.Context, transaction *state.Transaction,
	offer *actions.BodyOfAgreementOffer, now uint64) error {

	logger.Info(ctx, "Processing body of agreement offer")

	// First output must be the agent's locking script
	transaction.Lock()
	contractOutput := transaction.Output(0)
	agentLockingScript := a.LockingScript()
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

	if contract.Formation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	if contract.Formation.ContractExpiration != 0 && contract.Formation.ContractExpiration < now {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractExpired, "", now)), "reject")
	}

	if contract.Formation.BodyOfAgreementType != actions.ContractBodyOfAgreementTypeFull {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"Contract: BodyOfAgreementType: not Full", now)), "reject")
	}

	if contract.BodyOfAgreementFormation != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsAgreementExists, "", now)), "reject")
	}

	transaction.Lock()

	txid := transaction.TxID()

	firstInputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrap(err, "admin input output")
	}

	if !contract.IsAdminOrOperator(firstInputOutput.LockingScript) {
		transaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "", now)), "reject")
	}

	transaction.Unlock()

	// Create body of agreement formation response.
	logger.Info(ctx, "Accepting body of agreement offer")

	formation, err := offer.Formation()
	if err != nil {
		return errors.Wrap(err, "formation")
	}
	formation.Timestamp = now
	formation.Revision = 0

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

	// Finalize contract formation.
	contract.BodyOfAgreementFormation = formation
	contract.BodyOfAgreementFormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since all the balances were just settled.
	formationTransaction.Lock()
	formationTransaction.SetProcessed()
	formationTransaction.Unlock()

	if err := a.BroadcastTx(ctx, formationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return errors.Wrap(err, "post formation")
	}

	return nil
}

func (a *Agent) processBodyOfAgreementAmendment(ctx context.Context, transaction *state.Transaction,
	amendment *actions.BodyOfAgreementAmendment, now uint64) error {

	logger.Info(ctx, "Processing body of agreement amendment")

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

	contract.Lock()
	defer contract.Unlock()

	if contract.Formation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	if contract.BodyOfAgreementFormation == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsAgreementDoesNotExist, "", now)), "reject")
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

	if contract.BodyOfAgreementFormation.Revision != amendment.Revision {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsAgreementRevision, "", now)), "reject")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	vote, err := fetchReferenceVote(ctx, a.caches, agentLockingScript, amendment.RefTxID,
		a.IsTest(), now)
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

		if len(vote.Result.InstrumentCode) != 0 {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			instrumentID, _ := protocol.InstrumentIDForRaw(vote.Result.InstrumentType,
				vote.Result.InstrumentCode)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Vote For Instrument: %s", instrumentID), now)),
				"reject")
		}

		// Verify proposal amendments match these amendments.
		if len(vote.Result.ProposedAmendments) != len(amendment.Amendments) {
			vote.Unlock()
			a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment Count: Proposal %d, Amendment %d",
						len(vote.Result.ProposedAmendments), len(amendment.Amendments)), now)), "reject")
		}

		for i, proposedAmendment := range vote.Result.ProposedAmendments {
			if !proposedAmendment.Equal(amendment.Amendments[i]) {
				vote.Unlock()
				a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						fmt.Sprintf("RefTxID: Vote Result: Wrong Vote Amendment %d", i), now)),
					"reject")
			}
		}

		proposed = true
		proposalType = vote.Proposal.Type
		votingSystem = vote.Proposal.VoteSystem

		vote.Unlock()
		a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
	}

	// Copy formation to prevent modification of the original.
	isTest := a.IsTest()
	copyScript, err := protocol.Serialize(contract.BodyOfAgreementFormation, isTest)
	if err != nil {
		return errors.Wrap(err, "serialize body of agreement formation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return errors.Wrap(err, "deserialize body of agreement formation")
	}

	formation, ok := action.(*actions.BodyOfAgreementFormation)
	if !ok {
		return errors.New("BodyOfAgreementFormation script is wrong type")
	}

	if err := applyBodyOfAgreementAmendments(formation, contract.Formation.ContractPermissions,
		len(contract.Formation.VotingSystems), amendment.Amendments, proposed, proposalType,
		votingSystem, now); err != nil {
		if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
			return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
		}

		return errors.Wrap(err, "apply amendments")
	}

	logger.Info(ctx, "Accepting body of agreement amendment")

	formation.Revision = contract.BodyOfAgreementFormation.Revision + 1 // Bump the revision
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

	// Finalize contract formation.
	contract.BodyOfAgreementFormation = formation
	contract.BodyOfAgreementFormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since all the balances were just settled.
	formationTransaction.Lock()
	formationTransaction.SetProcessed()
	formationTransaction.Unlock()

	if err := a.BroadcastTx(ctx, formationTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return errors.Wrap(err, "post formation")
	}

	return nil
}

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context, transaction *state.Transaction,
	formation *actions.BodyOfAgreementFormation, now uint64) error {

	logger.Info(ctx, "Processing body of agreement formation")

	// First input must be the agent's locking script
	transaction.Lock()
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

	defer a.caches.Contracts.Save(ctx, a.contract)
	a.contract.Lock()
	defer a.contract.Unlock()

	if a.contract.BodyOfAgreementFormation != nil {
		if formation.Timestamp < a.contract.BodyOfAgreementFormation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp",
					int64(a.contract.BodyOfAgreementFormation.Timestamp)),
			}, "Older body of agreement formation")
			return nil
		} else if formation.Timestamp == a.contract.BodyOfAgreementFormation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp", int64(a.contract.Formation.Timestamp)),
			}, "Already processed body of agreement formation")
			return nil
		}
	}

	a.contract.BodyOfAgreementFormation = formation
	txid := transaction.GetTxID()
	a.contract.BodyOfAgreementFormationTxID = &txid
	a.contract.MarkModified()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Timestamp("timestamp", int64(formation.Timestamp)),
	}, "Updated body of agreement formation")

	return nil
}

// applyBodyOfAgreementAmendments applies the amendments to the body of agreement formation.
func applyBodyOfAgreementAmendments(bodyOfAgreementFormation *actions.BodyOfAgreementFormation,
	permissionBytes []byte, votingSystemsCount int, amendments []*actions.AmendmentField,
	proposed bool, proposalType, votingSystem uint32, now uint64) error {

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

		switch fip[0] {
		case actions.ContractFieldContractPermissions:
			if _, err := permissions.PermissionsFromBytes(amendment.Data,
				votingSystemsCount); err != nil {
				return platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("Amendments %d: Data: %s", i, err), now)
			}
		}

		fieldPermissions, err := bodyOfAgreementFormation.ApplyAmendment(fip, amendment.Operation,
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
	if err := bodyOfAgreementFormation.Validate(); err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Formation invalid after amendments: %s", err), now)
	}

	return nil
}
