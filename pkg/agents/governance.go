package agents

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/pkg/errors"
)

func (a *Agent) processProposal(ctx context.Context, transaction *transactions.Transaction,
	proposal *actions.Proposal, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()

	// First output must be the agent's locking script.
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
	transaction.Unlock()

	now := a.Now()

	if err := a.CheckContractIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	// Second output must be the agent's locking script.
	transaction.Lock()

	if transaction.OutputCount() < 2 {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"second contract output missing")
	}

	contractOutput1 := transaction.Output(1)
	if !agentLockingScript.Equal(contractOutput1.LockingScript) {
		transaction.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"second output must be to contract")
	}

	// TODO Verify the second output has enough funding for the Result action. --ce

	authorizingUnlockingScript := transaction.Input(0).UnlockingScript
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return nil, errors.Wrapf(err, "input locking script %d", 0)
	}
	firstInputLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	contract := a.Contract()
	contract.Lock()

	contractFee := contract.Formation.ContractFee

	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		contract.Unlock()
		return nil, errors.Wrap(err, "decode admin address")
	}

	contractPermissions := contract.Formation.ContractPermissions
	votingSystems := contract.Formation.VotingSystems

	contract.Unlock()

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return nil, errors.Wrap(err, "admin locking script")
	}

	// Check if sender is allowed to make proposal
	switch proposal.Type {
	case 0, 2: // 0 - Administration Proposal, 2 - Administrative Matter
		if !adminLockingScript.Equal(firstInputLockingScript) {
			return nil, platform.NewRejectError(actions.RejectionsNotAdministration, "")
		}

	case 1: // Holder Proposal
		// Sender must hold balance of at least one instrument
		hasBalance, err := state.HasAnyContractBalance(ctx, a.caches, contract,
			firstInputLockingScript)
		if err != nil {
			return nil, errors.Wrap(err, "has balance")
		}

		if !hasBalance {
			return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
				"proposer must be instrument holder")
		}

	default:
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "Type: unsupported")
	}

	if int(proposal.VoteSystem) >= len(votingSystems) {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "VoteSystem: out of range")
	}
	votingSystem := votingSystems[proposal.VoteSystem]

	if len(proposal.ProposedAmendments) > 0 && votingSystem.VoteType == "P" {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"Plurality votes not allowed for specific amendments")
	}

	// Validate messages vote related values
	if err := validateProposal(proposal, now); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	contractWideVote := false
	tokenQuantity := uint64(0)

	config := a.Config()
	if len(proposal.InstrumentCode) > 0 {
		instrumentHash20, err := bitcoin.NewHash20(proposal.InstrumentCode)
		if err != nil {
			return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("InstrumentCode: %s", err))
		}
		instrumentCode := state.InstrumentCode(*instrumentHash20)

		instrument, err := a.caches.Instruments.Get(ctx, agentLockingScript, instrumentCode)
		if err != nil {
			return nil, errors.Wrap(err, "get instrument")
		}

		if instrument == nil {
			return nil, platform.NewRejectError(actions.RejectionsInstrumentNotFound, "")
		}
		defer a.caches.Instruments.Release(ctx, agentLockingScript, instrumentCode)

		instrument.Lock()
		if instrument.Creation == nil {
			instrument.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsInstrumentNotFound, "creation missing")
		}

		switch proposal.Type {
		case 0: // Administration
			if !instrument.Creation.AdministrationProposal {
				instrument.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsInstrumentPermissions,
					"administration proposals not permitted")
			}
		case 1: // Holder
			if !instrument.Creation.HolderProposal {
				instrument.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsInstrumentPermissions,
					"holder proposals not permitted")
			}
		}
		instrument.Unlock()

		if proposal.Type == 1 { // Holder proposal
			// Check proposer balance
			balance, err := a.caches.Balances.Get(ctx, agentLockingScript, instrumentCode,
				firstInputLockingScript)
			if err != nil {
				return nil, errors.Wrap(err, "get balance")
			}

			if balance == nil {
				return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
					"proposer has no balance")
			}

			// A single balance lock doesn't need to use the balance locker since it isn't
			// susceptible to the group deadlock.
			balance.Lock()
			quantity := balance.Quantity
			balance.Unlock()
			a.caches.Balances.Release(ctx, agentLockingScript, instrumentCode, balance)

			if quantity == 0 {
				return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
					"proposer has no balance")
			}
		}

		if len(proposal.ProposedAmendments) > 0 {
			if proposal.VoteOptions != "AB" || proposal.VoteMax != 1 {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"single AB votes required for amendments")
			}

			// Validate proposed amendments.
			instrument.Lock()
			copyScript, err := protocol.Serialize(instrument.Creation, config.IsTest)
			instrument.Unlock()
			if err != nil {
				return nil, errors.Wrap(err, "serialize instrument creation")
			}

			action, err := protocol.Deserialize(copyScript, config.IsTest)
			if err != nil {
				return nil, errors.Wrap(err, "deserialize instrument creation")
			}

			creation, ok := action.(*actions.InstrumentCreation)
			if !ok {
				return nil, errors.New("InstrumentCreation script is wrong type")
			}

			creation.InstrumentRevision++
			creation.Timestamp = now

			if err := applyInstrumentAmendments(creation, contractPermissions, len(votingSystems),
				proposal.ProposedAmendments, true, proposal.Type, proposal.VoteSystem); err != nil {
				return nil, errors.Wrap(err, "apply amendments")
			}
		}
	} else {
		contract.Lock()
		if contract.Formation == nil {
			contract.Unlock()
			return nil, platform.NewRejectError(actions.RejectionsContractDoesNotExist,
				"formation missing")
		}

		switch proposal.Type {
		case 0: // Administration
			if !contract.Formation.AdministrationProposal {
				contract.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsContractPermissions,
					"administration proposals not permitted")
			}
		case 1: // Holder
			if !contract.Formation.HolderProposal {
				contract.Unlock()
				return nil, platform.NewRejectError(actions.RejectionsContractPermissions,
					"holder proposals not permitted")
			}
		}
		contract.Unlock()

		if len(proposal.ProposedAmendments) > 0 {
			if proposal.VoteOptions != "AB" || proposal.VoteMax != 1 {
				return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
					"single AB votes required for amendments")
			}

			// Validate proposed amendments.
			contract.Lock()
			copyScript, err := protocol.Serialize(contract.Formation, config.IsTest)
			contract.Unlock()
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

			formation.ContractRevision++
			formation.Timestamp = now

			if err := applyContractAmendments(formation, proposal.ProposedAmendments, true,
				proposal.Type, proposal.VoteSystem); err != nil {
				return nil, errors.Wrap(err, "apply amendments")
			}
		}
	}

	if len(proposal.ProposedAmendments) > 0 {
		// Check existing votes that have not been applied yet for conflicting fields.
		votes, err := a.caches.Votes.ListActive(ctx, a.store, agentLockingScript)
		if err != nil {
			return nil, errors.Wrap(err, "list votes")
		}
		defer a.caches.Votes.ReleaseMulti(ctx, agentLockingScript, votes)

		for _, vote := range votes {
			if vote.Proposal == nil || len(vote.Proposal.ProposedAmendments) == 0 {
				continue // Doesn't contain specific amendments
			}

			if len(proposal.InstrumentCode) > 0 {
				if len(vote.Proposal.InstrumentCode) == 0 ||
					proposal.InstrumentType != vote.Proposal.InstrumentType ||
					!bytes.Equal(proposal.InstrumentCode, vote.Proposal.InstrumentCode) {
					continue // Not an instrument amendment
				}
			} else {
				if len(vote.Proposal.InstrumentCode) != 0 {
					continue // Not a contract amendment
				}
			}

			// Determine if any fields conflict
			for _, field := range proposal.ProposedAmendments {
				for _, otherField := range vote.Proposal.ProposedAmendments {
					if bytes.Equal(field.FieldIndexPath, otherField.FieldIndexPath) {
						// Reject because of conflicting field amendment on active vote.
						return nil, platform.NewRejectError(actions.RejectionsProposalConflicts, "")
					}
				}
			}
		}
	}

	// Create vote response.
	logger.Info(ctx, "Accepting proposal")

	vote := &actions.Vote{
		Timestamp: now,
	}

	if err := vote.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	voteTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	if err := voteTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := voteTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	voteScript, err := protocol.Serialize(vote, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize vote")
	}

	voteScriptOutputIndex := len(voteTx.Outputs)
	if err := voteTx.AddOutput(voteScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add vote output")
	}

	// Add the contract fee and holder proposal fee.
	if proposal.Type == 1 { // Holder proposal
		contractFee += votingSystem.HolderProposalFee
	}

	if contractFee > 0 {
		if err := voteTx.AddOutput(a.FeeLockingScript(), contractFee, true, false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := voteTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if err := a.Sign(ctx, voteTx, a.FeeLockingScript()); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	voteTxID := *voteTx.MsgTx.TxHash()

	// Create vote data in storage.
	stateVote := &state.Vote{
		Proposal:         proposal,
		ProposalTxID:     &txid,
		VotingSystem:     votingSystem,
		Vote:             vote,
		VoteTxID:         &voteTxID,
		ContractWideVote: contractWideVote,
		TokenQuantity:    tokenQuantity,
	}
	addedVote, err := a.caches.Votes.Add(ctx, agentLockingScript, stateVote)
	if err != nil {
		return nil, errors.Wrap(err, "add vote")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	if addedVote != stateVote {
		return nil, errors.New("Vote already exists")
	}

	if err := stateVote.Prepare(ctx, a.caches, a.locker, contract, votingSystem,
		&now); err != nil {
		return nil, errors.Wrap(err, "prepare vote")
	}
	// TODO Figure out how to do voting balances for ballots. We want the Current balance at the
	// exact time the vote was timestamped. We can't update it here because the tx is already
	// signed.--ce
	// vote.Timestamp = now // now might have changed while locking balance

	voteTransaction, err := a.transactions.AddRaw(ctx, voteTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, voteTxID)

	// Set vote tx as processed since the contract is now formed.
	voteTransaction.Lock()
	voteTransaction.SetProcessed(a.ContractHash(), voteScriptOutputIndex)
	voteTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, voteTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(voteTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, nil, true, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	if a.scheduler != nil {
		cutOffTime := time.Unix(0, int64(proposal.VoteCutOffTimestamp))

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Timestamp("task_start", int64(proposal.VoteCutOffTimestamp)),
		}, "Scheduling finalize vote")

		task := NewFinalizeVoteTask(cutOffTime, a.agentStore, agentLockingScript, voteTxID)
		a.scheduler.Schedule(ctx, task)
	}

	return etx, nil
}

func (a *Agent) processVote(ctx context.Context, transaction *transactions.Transaction,
	vote *actions.Vote, outputIndex int) error {

	// First input must be the agent's locking script
	transaction.Lock()

	txid := transaction.TxID()

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}

	input := transaction.Input(0)
	proposalTxID := input.PreviousOutPoint.Hash

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputOutput.LockingScript) {
		transaction.Unlock()
		return nil // Not for this agent's contract
	}

	transaction.Unlock()

	if _, err := a.addResponseTxID(ctx, proposalTxID, txid, vote, outputIndex); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	stateVote := &state.Vote{
		Proposal:     nil,
		ProposalTxID: &proposalTxID,
		Vote:         vote,
		VoteTxID:     &txid,
	}
	addedVote, err := a.caches.Votes.Add(ctx, agentLockingScript, stateVote)
	if err != nil {
		return errors.Wrap(err, "add vote")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, txid)

	isNew := false
	if addedVote != stateVote {
		logger.Info(ctx, "Vote already exists")
	} else {
		logger.Info(ctx, "Vote created")
		isNew = true
	}

	addedVote.Lock()
	defer addedVote.Unlock()

	if addedVote.Vote == nil {
		addedVote.Vote = vote
		addedVote.VoteTxID = &txid
		addedVote.MarkModified()
	}

	config := a.Config()
	if addedVote.Proposal == nil {
		// Fetch proposal
		proposalTransaction, err := a.transactions.Get(ctx, proposalTxID)
		if err != nil {
			return errors.Wrap(err, "get proposal tx")
		}

		if proposalTransaction != nil {
			proposalTransaction.Lock()
			proposalOutputCount := proposalTransaction.OutputCount()
			for i := 0; i < proposalOutputCount; i++ {
				output := proposalTransaction.Output(i)

				action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
				if err != nil {
					continue
				}

				if p, ok := action.(*actions.Proposal); ok {
					addedVote.Proposal = p
					addedVote.MarkModified()
					break
				}
			}
			proposalTransaction.Unlock()

			if addedVote.Proposal == nil {
				logger.Error(ctx, "Proposal not found")
				return nil
			}
		}
	}

	if isNew {
		contract := a.Contract()
		contract.Lock()
		if contract.Formation == nil {
			contract.Unlock()
			return errors.Wrap(ErrNotImplemented, "vote response before contract")
		}
		votingSystems := contract.Formation.VotingSystems
		contract.Unlock()

		if int(addedVote.Proposal.VoteSystem) >= len(votingSystems) {
			return errors.New("VoteSystem: out of range")
		}
		votingSystem := votingSystems[addedVote.Proposal.VoteSystem]

		// TODO This might not get the right balances. It needs to have the balances from when the
		// vote was created. --ce
		now := a.Now()
		if err := addedVote.Prepare(ctx, a.caches, a.locker, contract, votingSystem,
			&now); err != nil {
			return errors.Wrap(err, "prepare vote")
		}
	}

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) processBallotCast(ctx context.Context, transaction *transactions.Transaction,
	ballotCast *actions.BallotCast, outputIndex int) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()

	// First output must be the agent's locking script.
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
		return nil, errors.Wrapf(err, "input locking script %d", 0)
	}
	lockingScript := inputOutput.LockingScript

	transaction.Unlock()

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("ballot_locking_script", lockingScript))

	now := a.Now()

	if err := a.CheckContractIsAvailable(now); err != nil {
		return nil, platform.NewDefaultRejectError(err)
	}

	voteTxIDHash, err := bitcoin.NewHash32(ballotCast.VoteTxId)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("VoteTxId: %s", err))
	}
	voteTxID := *voteTxIDHash
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID))

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return nil, platform.NewRejectError(actions.RejectionsVoteNotFound, "VoteTxId: not found")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	if vote.Result != nil {
		vote.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsVoteClosed, "")
	}

	if vote.Proposal == nil {
		vote.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsVoteNotFound, "Missing vote proposal")
	}

	if len(ballotCast.Vote) > int(vote.Proposal.VoteMax) {
		voteMax := vote.Proposal.VoteMax
		vote.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("Vote: more choices (%d) than VoteMax (%d)", len(ballotCast.Vote),
				voteMax))
	}
	vote.Unlock()

	ballot, err := a.caches.Ballots.Get(ctx, agentLockingScript, voteTxID, lockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get ballot")
	}

	if ballot == nil {
		return nil, platform.NewRejectError(actions.RejectionsUnauthorizedAddress, "Ballot not found")
	}
	defer a.caches.Ballots.Release(ctx, agentLockingScript, voteTxID, ballot)

	if isSigHashAll, err := authorizingUnlockingScript.IsSigHashAll(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, err.Error())
	} else if !isSigHashAll {
		return nil, platform.NewRejectError(actions.RejectionsSignatureNotSigHashAll, "")
	}

	ballot.Lock()
	if ballot.TxID != nil {
		ballot.Unlock()
		return nil, platform.NewRejectError(actions.RejectionsBallotAlreadyCounted,
			"Ballot already counted")
	}

	ballot.Vote = ballotCast.Vote
	quantity := ballot.Quantity
	ballot.Unlock()

	// Create ballot response.
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("quantity", quantity),
	}, "Accepting ballot")

	ballotCounted := &actions.BallotCounted{
		VoteTxId:  voteTxID.Bytes(),
		Vote:      ballotCast.Vote,
		Quantity:  quantity,
		Timestamp: now,
	}

	if err := ballotCounted.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	config := a.Config()
	ballotCountedTx := txbuilder.NewTxBuilder(float32(config.FeeRate), float32(config.DustFeeRate))

	if err := ballotCountedTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := ballotCountedTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	ballotCountedScript, err := protocol.Serialize(ballotCounted, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize ballot counted")
	}

	ballotCountedScriptOutputIndex := len(ballotCountedTx.Outputs)
	if err := ballotCountedTx.AddOutput(ballotCountedScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add ballot counted output")
	}

	// Add the contract fee and holder proposal fee.
	contract := a.Contract()
	contract.Lock()
	contractFee := contract.Formation.ContractFee
	contract.Unlock()
	if contractFee > 0 {
		if err := ballotCountedTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := ballotCountedTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if err := a.Sign(ctx, ballotCountedTx, a.FeeLockingScript()); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			return nil, platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
				err.Error())
		}

		return nil, errors.Wrap(err, "sign")
	}

	ballotCountedTxID := *ballotCountedTx.MsgTx.TxHash()

	ballot.Lock()
	ballot.TxID = &ballotCountedTxID
	ballot.Unlock()

	vote.Lock()
	vote.ApplyVote(ballotCast.Vote, quantity)
	vote.Unlock()

	ballotCountedTransaction, err := a.transactions.AddRaw(ctx, ballotCountedTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, ballotCountedTxID)

	// Set ballot counted tx as processed since the contract is now formed.
	ballotCountedTransaction.Lock()
	ballotCountedTransaction.SetProcessed(a.ContractHash(), ballotCountedScriptOutputIndex)
	ballotCountedTransaction.Unlock()

	transaction.Lock()
	transaction.AddResponseTxID(a.ContractHash(), outputIndex, ballotCountedTxID)
	tx := transaction.Tx.Copy()
	transaction.Unlock()

	etx, err := buildExpandedTx(ballotCountedTx.MsgTx, []*wire.MsgTx{&tx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := a.AddResponse(ctx, txid, []bitcoin.Script{lockingScript}, false, etx); err != nil {
		return etx, errors.Wrap(err, "respond")
	}

	return etx, nil
}

func (a *Agent) processBallotCounted(ctx context.Context, transaction *transactions.Transaction,
	ballotCounted *actions.BallotCounted, outputIndex int) error {

	// First input must be the agent's locking script
	transaction.Lock()
	txid := transaction.TxID()
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}
	inputLockingScript := inputOutput.LockingScript

	input := transaction.Input(0)
	ballotCastTxID := input.PreviousOutPoint.Hash
	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputLockingScript) {
		return nil // Not for this agent's contract
	}

	if _, err := a.addResponseTxID(ctx, ballotCastTxID, txid, ballotCounted,
		outputIndex); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	voteTxIDHash, err := bitcoin.NewHash32(ballotCounted.VoteTxId)
	if err != nil {
		return errors.Wrap(err, "VoteTxID")
	}
	voteTxID := *voteTxIDHash
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID))

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return errors.Wrap(err, "get vote")
	}

	if vote == nil {
		// We can't process the ballot before the vote so we would need to save this ballot until we
		// have processed the vote tx. This can only happen in recovery mode where transactions are
		// not being processed in the original order.
		return errors.Wrap(ErrNotImplemented, "Vote not found")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	if vote.Result != nil {
		vote.Unlock()
		return errors.New("Vote already complete")
	}

	if vote.Proposal == nil {
		vote.Unlock()
		return errors.New("Missing vote proposal")
	}

	ballotCastTransaction, err := a.transactions.Get(ctx, ballotCastTxID)
	if err != nil {
		return errors.Wrap(err, "get ballot cast tx")
	}

	if ballotCastTransaction == nil {
		return errors.New("Ballot cast transaction not found")
	}
	defer a.transactions.Release(ctx, ballotCastTxID)

	ballotCastTransaction.Lock()
	ballotCastInputOutput, err := transaction.InputOutput(0)
	if err != nil {
		ballotCastTransaction.Unlock()
		return errors.Wrapf(err, "ballot cast input locking script %d", 0)
	}
	lockingScript := ballotCastInputOutput.LockingScript
	ballotCastTransaction.Unlock()

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("ballot_locking_script", lockingScript))

	ballot, err := a.caches.Ballots.Get(ctx, agentLockingScript, voteTxID, lockingScript)
	if err != nil {
		return errors.Wrap(err, "get ballot")
	}

	if ballot == nil {
		return errors.New("Ballot not found")
	}
	defer a.caches.Ballots.Release(ctx, agentLockingScript, voteTxID, ballot)

	ballot.Lock()
	if ballot.TxID != nil {
		quantity := ballot.Quantity
		ballot.Unlock()
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Uint64("quantity", quantity),
		}, "Ballot already counted")
		return nil
	}

	ballot.TxID = &txid
	ballot.Vote = ballotCounted.Vote
	ballot.Unlock()
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("quantity", ballotCounted.Quantity),
	}, "Ballot counted")

	vote.Lock()
	vote.ApplyVote(ballotCounted.Vote, ballotCounted.Quantity)
	vote.Unlock()

	transaction.Lock()
	transaction.SetProcessed(a.ContractHash(), outputIndex)
	transaction.Unlock()

	return nil
}

func (a *Agent) processVoteResult(ctx context.Context, transaction *transactions.Transaction,
	result *actions.Result, outputIndex int) error {

	// First input must be the agent's locking script
	transaction.Lock()
	txid := transaction.TxID()
	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}
	inputLockingScript := inputOutput.LockingScript

	input := transaction.Input(0)
	voteTxID := input.PreviousOutPoint.Hash
	transaction.Unlock()

	agentLockingScript := a.LockingScript()
	if !agentLockingScript.Equal(inputLockingScript) {
		return nil // Not for this agent's contract
	}

	if _, err := a.addResponseTxID(ctx, voteTxID, txid, result, outputIndex); err != nil {
		return errors.Wrap(err, "add response txid")
	}

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return errors.Wrap(err, "get vote")
	}

	if vote == nil {
		// We can't process the result before the vote so we would need to save this result until we
		// have processed the vote tx. This can only happen in recovery mode where transactions are
		// not being processed in the original order.
		return errors.Wrap(ErrNotImplemented, "Vote not found")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	defer vote.Unlock()

	if vote.Result != nil {
		logger.Info(ctx, "Vote result already processed")
		return nil
	}

	vote.Result = result
	vote.ResultTxID = &txid
	vote.MarkModified()

	return nil
}

// fetchReferenceVote fetches a vote for a reference txid provided in an amendment or modification.
// It returns nil if the txid is empty, or an error if the vote is not found or not complete.
func fetchReferenceVote(ctx context.Context, caches *state.Caches,
	transactions *transactions.TransactionCache, contractLockingScript bitcoin.Script, txid []byte,
	isTest bool) (*state.Vote, error) {

	if len(txid) == 0 {
		return nil, nil
	}

	refTxID, err := bitcoin.NewHash32(txid)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "RefTxID")
	}

	// Retrieve Vote Result
	voteResultTransaction, err := transactions.Get(ctx, *refTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get ref tx")
	}

	if voteResultTransaction == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result Tx Not Found")
	}
	defer transactions.Release(ctx, *refTxID)

	var voteResult *actions.Result
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
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result Not Found")
	}

	// Retrieve Vote
	voteTxID, err := bitcoin.NewHash32(voteResult.VoteTxId)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: VoteTxId: %s", err))
	}

	// Retrieve the vote
	vote, err := caches.Votes.Get(ctx, contractLockingScript, *voteTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return nil, platform.NewRejectError(actions.RejectionsVoteNotFound,
			"RefTxID: Vote Result: Vote Data Not Found")
	}

	vote.Lock()

	if vote.Proposal == nil || vote.Result == nil {
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result: Vote Not Completed")
	}

	if vote.Result.Result != "A" {
		result := vote.Result.Result
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: Vote Not Accepted: %s", result))
	}

	vote.Unlock()
	return vote, nil
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

// validateProposal returns true if the Proposal is valid.
func validateProposal(proposal *actions.Proposal, now uint64) error {
	if len(proposal.VoteOptions) == 0 {
		return errors.New("No vote options")
	}

	if proposal.VoteMax == 0 {
		return errors.New("Zero vote max")
	}

	if proposal.VoteCutOffTimestamp < now {
		return fmt.Errorf("Vote Expired : %d < %d", proposal.VoteCutOffTimestamp, now)
	}

	return nil
}
