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
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processBodyOfAgreementOffer(ctx context.Context, transaction *state.Transaction,
	offer *actions.BodyOfAgreementOffer, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	// First output must be the agent's locking script
	transaction.Lock()
	contractOutput := transaction.Output(0)
	agentLockingScript := a.LockingScript()
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

	if err := contract.CheckIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	if contract.Formation.BodyOfAgreementType != actions.ContractBodyOfAgreementTypeFull {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"Contract: BodyOfAgreementType: not Full")
	}

	if contract.BodyOfAgreementFormation != nil {
		return nil, platform.NewRejectError(actions.RejectionsAgreementExists, "")
	}

	transaction.Lock()

	txid := transaction.TxID()

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	firstInputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrap(err, "admin input output")
	}
	authorizingLockingScript := firstInputOutput.LockingScript

	transaction.Unlock()

	if !contract.IsAdminOrOperator(authorizingLockingScript) {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "")
	}

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	// Create body of agreement formation response.
	logger.Info(ctx, "Accepting body of agreement offer")

	formation, err := offer.Formation()
	if err != nil {
		return nil, errors.Wrap(err, "formation")
	}
	formation.Timestamp = now
	formation.Revision = 0

	if err := formation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	formationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, a.IsTest())
	if err != nil {
		return nil, errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
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
	contract.BodyOfAgreementFormation = formation
	contract.BodyOfAgreementFormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the body of agreement is now formed.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.Respond(ctx, txid, formationTransaction); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return etx, errors.Wrap(err, "post formation")
	}

	return etx, nil
}

func (a *Agent) processBodyOfAgreementAmendment(ctx context.Context, transaction *state.Transaction,
	amendment *actions.BodyOfAgreementAmendment, outputIndex int) (*expanded_tx.ExpandedTx, error) {

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
	defer a.caches.Contracts.Save(ctx, a.contract)
	contract.Lock()
	defer contract.Unlock()

	now := a.Now()

	if err := contract.CheckIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	if contract.BodyOfAgreementFormation == nil {
		return nil, platform.NewRejectError(actions.RejectionsAgreementDoesNotExist, "")
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

	if contract.BodyOfAgreementFormation.Revision != amendment.Revision {
		return nil, platform.NewRejectError(actions.RejectionsAgreementRevision, "")
	}

	// Check proposal if there was one
	proposed := false
	proposalType := uint32(0)
	votingSystem := uint32(0)

	vote, err := fetchReferenceVote(ctx, a.caches, agentLockingScript, amendment.RefTxID,
		a.IsTest())
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
		a.caches.Votes.Release(ctx, agentLockingScript, *vote.VoteTxID)
	}

	// Copy formation to prevent modification of the original.
	isTest := a.IsTest()
	copyScript, err := protocol.Serialize(contract.BodyOfAgreementFormation, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize body of agreement formation")
	}

	action, err := protocol.Deserialize(copyScript, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize body of agreement formation")
	}

	formation, ok := action.(*actions.BodyOfAgreementFormation)
	if !ok {
		return nil, errors.New("BodyOfAgreementFormation script is wrong type")
	}

	if err := applyBodyOfAgreementAmendments(formation, contract.Formation.ContractPermissions,
		len(contract.Formation.VotingSystems), amendment.Amendments, proposed, proposalType,
		votingSystem); err != nil {
		return nil, errors.Wrap(err, "apply amendments")
	}

	logger.Info(ctx, "Accepting body of agreement amendment")

	formation.Revision = contract.BodyOfAgreementFormation.Revision + 1 // Bump the revision
	formation.Timestamp = now

	if err := formation.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	formationTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := formationTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := formationTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	formationScript, err := protocol.Serialize(formation, isTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize formation")
	}

	formationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(formationScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add formation output")
	}

	// Add the contract fee.
	contractFee := contract.Formation.ContractFee
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
	contract.BodyOfAgreementFormation = formation
	contract.BodyOfAgreementFormationTxID = formationTx.MsgTx.TxHash()
	contract.MarkModified()

	formationTxID := *formationTx.MsgTx.TxHash()
	formationTransaction, err := a.caches.Transactions.AddRaw(ctx, formationTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, formationTxID)

	// Set formation tx as processed since the body of agreement is now amended.
	formationTransaction.Lock()
	formationTransaction.SetProcessed(a.ContractHash(), formationScriptOutputIndex)
	formationTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, formationTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(formationTx.MsgTx, []*wire.MsgTx{tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.Respond(ctx, txid, formationTransaction); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, formationTransaction); err != nil {
		return etx, errors.Wrap(err, "post formation")
	}

	return etx, nil
}

func (a *Agent) processBodyOfAgreementFormation(ctx context.Context, transaction *state.Transaction,
	formation *actions.BodyOfAgreementFormation, outputIndex int) error {

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
		return nil // Not for this agent's contract
	}

	if _, err := a.addResponseTxID(ctx, input.PreviousOutPoint.Hash, txid); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	contract := a.Contract()
	defer a.caches.Contracts.Save(ctx, contract)
	contract.Lock()
	defer contract.Unlock()

	if contract.BodyOfAgreementFormation != nil {
		if formation.Timestamp < contract.BodyOfAgreementFormation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp",
					int64(contract.BodyOfAgreementFormation.Timestamp)),
			}, "Older body of agreement formation")
			return nil
		} else if formation.Timestamp == contract.BodyOfAgreementFormation.Timestamp {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("timestamp", int64(formation.Timestamp)),
				logger.Timestamp("existing_timestamp", int64(contract.Formation.Timestamp)),
			}, "Already processed body of agreement formation")
			return nil
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("timestamp", int64(formation.Timestamp)),
		}, "Updating body of agreement formation")
	} else {
		logger.Info(ctx, "New body of agreement formation")
	}

	contract.BodyOfAgreementFormation = formation
	contract.BodyOfAgreementFormationTxID = &txid
	contract.MarkModified()

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

// applyBodyOfAgreementAmendments applies the amendments to the body of agreement formation.
func applyBodyOfAgreementAmendments(bodyOfAgreementFormation *actions.BodyOfAgreementFormation,
	permissionBytes []byte, votingSystemsCount int, amendments []*actions.AmendmentField,
	proposed bool, proposalType, votingSystem uint32) error {

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

		fieldPermissions, err := bodyOfAgreementFormation.ApplyAmendment(fip, amendment.Operation,
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
	if err := bodyOfAgreementFormation.Validate(); err != nil {
		return platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Body of agreement invalid after amendments: %s", err))
	}

	return nil
}
