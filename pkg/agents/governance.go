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
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

func (a *Agent) processProposal(ctx context.Context, transaction *state.Transaction,
	proposal *actions.Proposal, now uint64) error {

	logger.Info(ctx, "Processing proposal")

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
		return nil // Not for this agent's contract
	}
	transaction.Unlock()

	if !a.ContractExists() {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	if movedTxID := a.MovedTxID(); movedTxID != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractMoved, movedTxID.String(), now)),
			"reject")
	}

	// Check if contract is frozen.
	if a.ContractIsExpired(now) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractExpired, "", now)), "reject")
	}

	// Second output must be the agent's locking script.
	transaction.Lock()

	if transaction.OutputCount() < 2 {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"second contract output missing", now)), "reject")
	}

	contractOutput1 := transaction.Output(1)
	if !agentLockingScript.Equal(contractOutput1.LockingScript) {
		transaction.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"second output must be to contract", now)), "reject")
	}

	// TODO Verify the second output has enough funding for the Result action. --ce

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}
	firstInputLockingScript := inputOutput.LockingScript

	transaction.Unlock()

	contract := a.Contract()
	contract.Lock()

	contractFee := contract.Formation.ContractFee

	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		contract.Unlock()
		return errors.Wrap(err, "decode admin address")
	}

	contractPermissions := contract.Formation.ContractPermissions
	votingSystems := contract.Formation.VotingSystems

	contract.Unlock()

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		return errors.Wrap(err, "admin locking script")
	}

	// Check if sender is allowed to make proposal
	switch proposal.Type {
	case 0, 2: // 0 - Administration Proposal, 2 - Administrative Matter
		if !adminLockingScript.Equal(firstInputLockingScript) {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsNotAdministration, "", now)), "reject")
		}

	case 1: // Holder Proposal
		// Sender must hold balance of at least one instrument
		hasBalance, err := state.HasAnyContractBalance(ctx, a.caches, contract,
			firstInputLockingScript)
		if err != nil {
			return errors.Wrap(err, "has balance")
		}

		if !hasBalance {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
					"proposer must be instrument holder", now)), "reject")
		}

	default:
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "Type: unsupported", now)),
			"reject")
	}

	if int(proposal.VoteSystem) >= len(votingSystems) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "VoteSystem: out of range",
				now)), "reject")
	}
	votingSystem := votingSystems[proposal.VoteSystem]

	if len(proposal.ProposedAmendments) > 0 && votingSystem.VoteType == "P" {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				"Plurality votes not allowed for specific amendments",
				now)), "reject")
	}

	// Validate messages vote related values
	if err := validateProposal(proposal, now); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error(), now)), "reject")
	}

	contractWideVote := false
	tokenQuantity := uint64(0)

	if len(proposal.InstrumentCode) > 0 {
		instrumentHash20, err := bitcoin.NewHash20(proposal.InstrumentCode)
		if err != nil {
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsMsgMalformed,
					fmt.Sprintf("InstrumentCode: %s", err), now)), "reject")
		}
		instrumentCode := state.InstrumentCode(*instrumentHash20)

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
		if instrument.Creation == nil {
			instrument.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInstrumentNotFound, "creation missing",
					now)), "reject")
		}

		switch proposal.Type {
		case 0: // Administration
			if !instrument.Creation.AdministrationProposal {
				instrument.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsInstrumentPermissions,
						"administration proposals not permitted", now)), "reject")
			}
		case 1: // Holder
			if !instrument.Creation.HolderProposal {
				instrument.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsInstrumentPermissions,
						"holder proposals not permitted", now)), "reject")
			}
		}
		instrument.Unlock()

		if proposal.Type == 1 { // Holder proposal
			// Check proposer balance
			balance, err := a.caches.Balances.Get(ctx, agentLockingScript, instrumentCode,
				firstInputLockingScript)
			if err != nil {
				return errors.Wrap(err, "get balance")
			}

			if balance == nil {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
						"proposer has no balance", now)), "reject")
			}

			balance.Lock()
			quantity := balance.Quantity
			balance.Unlock()
			a.caches.Balances.Release(ctx, agentLockingScript, instrumentCode, balance)

			if quantity == 0 {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsUnauthorizedAddress,
						"proposer has no balance", now)), "reject")
			}
		}

		if len(proposal.ProposedAmendments) > 0 {
			if proposal.VoteOptions != "AB" || proposal.VoteMax != 1 {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						"single AB votes required for amendments", now)), "reject")
			}

			// Validate proposed amendments.
			isTest := a.IsTest()
			instrument.Lock()
			copyScript, err := protocol.Serialize(instrument.Creation, isTest)
			instrument.Unlock()
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

			creation.InstrumentRevision++
			creation.Timestamp = now

			if err := applyInstrumentAmendments(creation, contractPermissions, len(votingSystems),
				proposal.ProposedAmendments, true, proposal.Type, proposal.VoteSystem,
				now); err != nil {
				if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
					return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
				}

				return errors.Wrap(err, "apply amendments")
			}
		}
	} else {
		contract.Lock()
		if contract.Formation == nil {
			contract.Unlock()
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsContractDoesNotExist, "formation missing",
					now)), "reject")
		}

		switch proposal.Type {
		case 0: // Administration
			if !contract.Formation.AdministrationProposal {
				contract.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsContractPermissions,
						"administration proposals not permitted", now)), "reject")
			}
		case 1: // Holder
			if !contract.Formation.HolderProposal {
				contract.Unlock()
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsContractPermissions,
						"holder proposals not permitted", now)), "reject")
			}
		}
		contract.Unlock()

		if len(proposal.ProposedAmendments) > 0 {
			if proposal.VoteOptions != "AB" || proposal.VoteMax != 1 {
				return errors.Wrap(a.sendRejection(ctx, transaction,
					platform.NewRejectError(actions.RejectionsMsgMalformed,
						"single AB votes required for amendments", now)), "reject")
			}

			// Validate proposed amendments.
			isTest := a.IsTest()
			contract.Lock()
			copyScript, err := protocol.Serialize(contract.Formation, isTest)
			contract.Unlock()
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

			formation.ContractRevision++
			formation.Timestamp = now

			if err := applyContractAmendments(formation, proposal.ProposedAmendments, true,
				proposal.Type, proposal.VoteSystem, now); err != nil {
				if rejectError, ok := errors.Cause(err).(platform.RejectError); ok {
					return errors.Wrap(a.sendRejection(ctx, transaction, rejectError), "reject")
				}

				return errors.Wrap(err, "apply amendments")
			}
		}
	}

	if len(proposal.ProposedAmendments) > 0 {
		// Check existing votes that have not been applied yet for conflicting fields.
		votes, err := a.caches.Votes.ListActive(ctx, a.store, agentLockingScript)
		if err != nil {
			return errors.Wrap(err, "list votes")
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
						return errors.Wrap(a.sendRejection(ctx, transaction,
							platform.NewRejectError(actions.RejectionsProposalConflicts,
								"", now)), "reject")
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

	voteTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := voteTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := voteTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	voteScript, err := protocol.Serialize(vote, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize vote")
	}

	if err := voteTx.AddOutput(voteScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add vote output")
	}

	// Add the contract fee and holder proposal fee.
	if proposal.Type == 1 { // Holder proposal
		contractFee += votingSystem.HolderProposalFee
	}

	if contractFee > 0 {
		if err := voteTx.AddOutput(a.FeeLockingScript(), contractFee, true, false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := voteTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if _, err := voteTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
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
		return errors.Wrap(err, "add vote")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	if addedVote != stateVote {
		return errors.New("Vote already exists")
	}

	if err := stateVote.Prepare(ctx, a.caches, contract, votingSystem); err != nil {
		return errors.Wrap(err, "prepare vote")
	}

	voteTransaction, err := a.caches.Transactions.AddRaw(ctx, voteTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, voteTxID)

	// Set vote tx as processed since the contract is now formed.
	voteTransaction.Lock()
	voteTransaction.SetProcessed()
	voteTransaction.Unlock()

	if err := a.BroadcastTx(ctx, voteTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, voteTransaction); err != nil {
		return errors.Wrap(err, "post vote")
	}

	if a.scheduler != nil {
		cutOffTime := time.Unix(0, int64(proposal.VoteCutOffTimestamp))
		a.scheduler.Schedule(ctx, voteTxID, cutOffTime,
			func(ctx context.Context, interrupt <-chan interface{}) error {
				return FinalizeVote(ctx, a.factory, agentLockingScript, voteTxID,
					proposal.VoteCutOffTimestamp)
			})
	}

	return nil
}

func (a *Agent) processVote(ctx context.Context, transaction *state.Transaction,
	vote *actions.Vote, now uint64) error {

	logger.Info(ctx, "Processing vote")

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

	if addedVote.Proposal == nil {
		// Fetch proposal
		proposalTransaction, err := a.caches.Transactions.Get(ctx, proposalTxID)
		if err != nil {
			return errors.Wrap(err, "get proposal tx")
		}

		if proposalTransaction != nil {
			isTest := a.IsTest()
			proposalTransaction.Lock()
			proposalOutputCount := proposalTransaction.OutputCount()
			for i := 0; i < proposalOutputCount; i++ {
				output := proposalTransaction.Output(i)

				action, err := protocol.Deserialize(output.LockingScript, isTest)
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
			return errors.New("Missing contract formation")
		}
		votingSystems := contract.Formation.VotingSystems
		contract.Unlock()

		if int(addedVote.Proposal.VoteSystem) >= len(votingSystems) {
			return errors.New("VoteSystem: out of range")
		}
		votingSystem := votingSystems[addedVote.Proposal.VoteSystem]

		if err := addedVote.Prepare(ctx, a.caches, contract, votingSystem); err != nil {
			return errors.Wrap(err, "prepare vote")
		}
	}

	return nil
}

func (a *Agent) processBallotCast(ctx context.Context, transaction *state.Transaction,
	ballotCast *actions.BallotCast, now uint64) error {

	logger.Info(ctx, "Processing ballot cast")

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
		return nil // Not for this agent's contract
	}

	inputOutput, err := transaction.InputOutput(0)
	if err != nil {
		transaction.Unlock()
		return errors.Wrapf(err, "input locking script %d", 0)
	}
	lockingScript := inputOutput.LockingScript

	transaction.Unlock()

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("ballot_locking_script", lockingScript))

	if !a.ContractExists() {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractDoesNotExist, "", now)), "reject")
	}

	if movedTxID := a.MovedTxID(); movedTxID != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractMoved, movedTxID.String(), now)),
			"reject")
	}

	// Check if contract is frozen.
	if a.ContractIsExpired(now) {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsContractExpired, "", now)), "reject")
	}

	voteTxIDHash, err := bitcoin.NewHash32(ballotCast.VoteTxId)
	if err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("VoteTxId: %s", err), now)), "reject")
	}
	voteTxID := *voteTxIDHash
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID))

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsVoteNotFound, "VoteTxId: not found", now)),
			"reject")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	if vote.Result != nil {
		vote.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsVoteClosed, "", now)), "reject")
	}

	if vote.Proposal == nil {
		vote.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsVoteNotFound, "Missing vote proposal", now)),
			"reject")
	}

	if len(ballotCast.Vote) > int(vote.Proposal.VoteMax) {
		voteMax := vote.Proposal.VoteMax
		vote.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed,
				fmt.Sprintf("Vote: more choices (%d) than VoteMax (%d)", len(ballotCast.Vote),
					voteMax), now)), "reject")
	}
	vote.Unlock()

	ballot, err := a.caches.Ballots.Get(ctx, agentLockingScript, voteTxID, lockingScript)
	if err != nil {
		return errors.Wrap(err, "get ballot")
	}

	if ballot == nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, "Ballot not found", now)),
			"reject")
	}
	defer a.caches.Ballots.Release(ctx, agentLockingScript, voteTxID, ballot)

	ballot.Lock()
	if ballot.TxID != nil {
		ballot.Unlock()
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsBallotAlreadyCounted,
				"Ballot already counted", now)), "reject")
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

	ballotCountedTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := ballotCountedTx.AddInput(wire.OutPoint{Hash: txid, Index: 0}, agentLockingScript,
		contractOutput.Value); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := ballotCountedTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	ballotCountedScript, err := protocol.Serialize(ballotCounted, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize ballot counted")
	}

	if err := ballotCountedTx.AddOutput(ballotCountedScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add ballot counted output")
	}

	// Add the contract fee and holder proposal fee.
	contract := a.Contract()
	contract.Lock()
	contractFee := contract.Formation.ContractFee
	contract.Unlock()
	if contractFee > 0 {
		if err := ballotCountedTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := ballotCountedTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if _, err := ballotCountedTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		if errors.Cause(err) == txbuilder.ErrInsufficientValue {
			logger.Warn(ctx, "Insufficient tx funding : %s", err)
			return errors.Wrap(a.sendRejection(ctx, transaction,
				platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding, err.Error(),
					now)), "reject")
		}

		return errors.Wrap(err, "sign")
	}

	ballotCountedTxID := *ballotCountedTx.MsgTx.TxHash()

	ballot.Lock()
	ballot.TxID = &ballotCountedTxID
	ballot.Unlock()

	vote.Lock()
	vote.ApplyVote(ballotCast.Vote, quantity)
	vote.Unlock()

	ballotCountedTransaction, err := a.caches.Transactions.AddRaw(ctx, ballotCountedTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, ballotCountedTxID)

	// Set ballot counted tx as processed since the contract is now formed.
	ballotCountedTransaction.Lock()
	ballotCountedTransaction.SetProcessed()
	ballotCountedTransaction.Unlock()

	if err := a.BroadcastTx(ctx, ballotCountedTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToSubscriptions(ctx, []bitcoin.Script{lockingScript},
		ballotCountedTransaction); err != nil {
		return errors.Wrap(err, "post ballot counted")
	}

	return nil
}

func (a *Agent) processBallotCounted(ctx context.Context, transaction *state.Transaction,
	ballotCounted *actions.BallotCounted, now uint64) error {

	logger.Info(ctx, "Processing ballot counted")

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
		return errors.New("Vote not found")
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

	ballotCastTransaction, err := a.caches.Transactions.Get(ctx, ballotCastTxID)
	if err != nil {
		return errors.Wrap(err, "get ballot cast tx")
	}

	if ballotCastTransaction == nil {
		return errors.New("Ballot cast transaction not found")
	}
	defer a.caches.Transactions.Release(ctx, ballotCastTxID)

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

	return nil
}

func FinalizeVote(ctx context.Context, factory AgentFactory, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, now uint64) error {

	agent, err := factory.GetAgent(ctx, contractLockingScript)
	if err != nil {
		return errors.Wrap(err, "get agent")
	}

	if agent == nil {
		return errors.New("Agent not found")
	}
	defer factory.ReleaseAgent(ctx, agent)

	return agent.finalizeVote(ctx, voteTxID, now)
}

func (a *Agent) finalizeVote(ctx context.Context, voteTxID bitcoin.Hash32, now uint64) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID))

	logger.Info(ctx, "Finalizing vote")

	agentLockingScript := a.LockingScript()
	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return errors.New("Vote not found")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	defer vote.Unlock()

	if vote.Result != nil {
		return errors.New("Vote already complete")
	}

	if vote.Proposal == nil {
		return errors.New("Missing vote proposal")
	}

	if vote.ProposalTxID == nil {
		return errors.New("Missing vote proposal txid")
	}

	tally, result := vote.CalculateResults(ctx)

	proposalTxID := *vote.ProposalTxID

	proposalTransaction, err := a.caches.Transactions.Get(ctx, proposalTxID)
	if err != nil {
		return errors.Wrap(err, "get proposal tx")
	}

	if proposalTransaction == nil {
		return errors.New("Proposal transaction not found")
	}
	defer a.caches.Transactions.Release(ctx, proposalTxID)

	proposalTransaction.Lock()
	contractOutput := proposalTransaction.Output(1)
	if !contractOutput.LockingScript.Equal(agentLockingScript) {
		proposalTransaction.Unlock()
		return errors.New("Proposal result output not to contract")
	}
	proposalOutputValue := contractOutput.Value
	proposalTransaction.Unlock()

	// Create vote result.
	voteResult := &actions.Result{
		InstrumentType:     vote.Proposal.InstrumentType,
		InstrumentCode:     vote.Proposal.InstrumentCode,
		ProposedAmendments: vote.Proposal.ProposedAmendments,
		VoteTxId:           voteTxID.Bytes(),
		OptionTally:        tally,
		Result:             result,
		Timestamp:          now,
	}

	voteResultTx := txbuilder.NewTxBuilder(a.FeeRate(), a.DustFeeRate())

	if err := voteResultTx.AddInput(wire.OutPoint{Hash: proposalTxID, Index: 1}, agentLockingScript,
		proposalOutputValue); err != nil {
		return errors.Wrap(err, "add input")
	}

	if err := voteResultTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return errors.Wrap(err, "add contract output")
	}

	voteResultScript, err := protocol.Serialize(voteResult, a.IsTest())
	if err != nil {
		return errors.Wrap(err, "serialize vote result")
	}

	if err := voteResultTx.AddOutput(voteResultScript, 0, false, false); err != nil {
		return errors.Wrap(err, "add vote result output")
	}

	// Add the contract fee and holder proposal fee.
	contract := a.Contract()
	contract.Lock()
	contractFee := contract.Formation.ContractFee
	contract.Unlock()
	if contractFee > 0 {
		if err := voteResultTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return errors.Wrap(err, "add contract fee")
		}
	} else if err := voteResultTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if _, err := voteResultTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		return errors.Wrap(err, "sign")
	}

	voteResultTxID := *voteResultTx.MsgTx.TxHash()

	vote.Result = voteResult
	vote.ResultTxID = &voteResultTxID

	voteResultTransaction, err := a.caches.Transactions.AddRaw(ctx, voteResultTx.MsgTx, nil)
	if err != nil {
		return errors.Wrap(err, "add response tx")
	}
	defer a.caches.Transactions.Release(ctx, voteResultTxID)

	// Set vote result tx as processed since the contract is now formed.
	voteResultTransaction.Lock()
	voteResultTransaction.SetProcessed()
	voteResultTransaction.Unlock()

	if err := a.BroadcastTx(ctx, voteResultTx.MsgTx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, voteResultTransaction); err != nil {
		return errors.Wrap(err, "post vote result")
	}

	return nil
}

func (a *Agent) processVoteResult(ctx context.Context, transaction *state.Transaction,
	result *actions.Result, now uint64) error {

	logger.Info(ctx, "Processing vote result")

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

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return errors.New("Vote not found")
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
	contractLockingScript bitcoin.Script, txid []byte, isTest bool,
	now uint64) (*state.Vote, error) {

	if len(txid) == 0 {
		return nil, nil
	}

	refTxID, err := bitcoin.NewHash32(txid)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, "RefTxID", now)
	}

	// Retrieve Vote Result
	voteResultTransaction, err := caches.Transactions.Get(ctx, *refTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get ref tx")
	}

	if voteResultTransaction == nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result Tx Not Found", now)
	}
	defer caches.Transactions.Release(ctx, *refTxID)

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
			"RefTxID: Vote Result Not Found", now)
	}

	// Retrieve Vote
	voteTxID, err := bitcoin.NewHash32(voteResult.VoteTxId)
	if err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: VoteTxId: %s", err), now)
	}

	// Retrieve the vote
	vote, err := caches.Votes.Get(ctx, contractLockingScript, *voteTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return nil, platform.NewRejectError(actions.RejectionsVoteNotFound,
			"RefTxID: Vote Result: Vote Data Not Found", now)
	}

	vote.Lock()

	if vote.Proposal == nil || vote.Result == nil {
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			"RefTxID: Vote Result: Vote Not Completed", now)
	}

	if vote.Result.Result != "A" {
		result := vote.Result.Result
		vote.Unlock()
		caches.Votes.Release(ctx, contractLockingScript, *voteTxID)
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed,
			fmt.Sprintf("RefTxID: Vote Result: Vote Not Accepted: %s", result), now)
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
