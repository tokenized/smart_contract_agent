package agents

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// FinalizeVoteTask executes when the specified cut off time for the proposal is reached, then
// calculates the result of the vote by counting all the ballots and broadcasts the Result action.
type FinalizeVoteTask struct {
	start time.Time

	store                 Store
	contractLockingScript bitcoin.Script
	voteTxID              bitcoin.Hash32

	lock sync.Mutex
}

func NewFinalizeVoteTask(start time.Time, store Store,
	contractLockingScript bitcoin.Script, voteTxID bitcoin.Hash32) *FinalizeVoteTask {

	return &FinalizeVoteTask{
		start:                 start,
		store:                 store,
		contractLockingScript: contractLockingScript,
		voteTxID:              voteTxID,
	}
}

func (t *FinalizeVoteTask) ID() bitcoin.Hash32 {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.voteTxID
}

func (t *FinalizeVoteTask) Start() time.Time {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.start
}

func (t *FinalizeVoteTask) ContractLockingScript() bitcoin.Script {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.contractLockingScript
}

func (t *FinalizeVoteTask) Run(ctx context.Context,
	interrupt <-chan interface{}) (*expanded_tx.ExpandedTx, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	store := t.store
	contractLockingScript := t.contractLockingScript
	voteTxID := t.voteTxID

	return FinalizeVote(ctx, store, contractLockingScript, voteTxID)
}

func FinalizeVote(ctx context.Context, store Store, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("vote_txid", voteTxID),
		logger.Stringer("contract_locking_script", contractLockingScript),
	}, "Finalizing vote")

	agent, err := store.GetAgent(ctx, contractLockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get agent")
	}

	if agent == nil {
		return nil, errors.New("Agent not found")
	}
	defer agent.Release(ctx)

	return agent.FinalizeVote(ctx, voteTxID)
}

func (a *Agent) FinalizeVote(ctx context.Context,
	voteTxID bitcoin.Hash32) (*expanded_tx.ExpandedTx, error) {

	agentLockingScript := a.LockingScript()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID),
		logger.Stringer("contract_locking_script", agentLockingScript))

	logger.Info(ctx, "Finalizing vote")

	vote, err := a.caches.Votes.Get(ctx, agentLockingScript, voteTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get vote")
	}

	if vote == nil {
		return nil, errors.New("Vote not found")
	}
	defer a.caches.Votes.Release(ctx, agentLockingScript, voteTxID)

	vote.Lock()
	defer vote.Unlock()

	if vote.Result != nil {
		return nil, errors.New("Vote already complete")
	}

	if vote.Proposal == nil {
		return nil, errors.New("Missing vote proposal")
	}

	if vote.ProposalTxID == nil {
		return nil, errors.New("Missing vote proposal txid")
	}

	tally, result := vote.CalculateResults(ctx)

	proposalTxID := *vote.ProposalTxID

	proposalTransaction, err := a.transactions.Get(ctx, proposalTxID)
	if err != nil {
		return nil, errors.Wrap(err, "get proposal tx")
	}

	if proposalTransaction == nil {
		return nil, errors.New("Proposal transaction not found")
	}
	defer a.transactions.Release(ctx, proposalTxID)

	proposalTransaction.Lock()

	contractOutput := proposalTransaction.Output(1)
	if !contractOutput.LockingScript.Equal(agentLockingScript) {
		proposalTransaction.Unlock()
		return nil, errors.New("Proposal result output not to contract")
	}
	proposalOutputValue := contractOutput.Value

	config := a.Config()
	proposalOutputCount := proposalTransaction.OutputCount()
	outputIndex := 0
	for i := 0; i < proposalOutputCount; i++ {
		output := proposalTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, config.IsTest)
		if err != nil {
			continue
		}

		if _, ok := action.(*actions.Proposal); ok {
			outputIndex = i
			break
		}
	}

	proposalTransaction.Unlock()

	// Create vote result.
	voteResult := &actions.Result{
		InstrumentType:     vote.Proposal.InstrumentType,
		InstrumentCode:     vote.Proposal.InstrumentCode,
		ProposedAmendments: vote.Proposal.ProposedAmendments,
		VoteTxId:           voteTxID.Bytes(),
		OptionTally:        tally,
		Result:             result,
		Timestamp:          a.Now(),
	}

	if err := voteResult.Validate(); err != nil {
		return nil, platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error())
	}

	voteResultTx := txbuilder.NewTxBuilder(config.FeeRate, config.DustFeeRate)

	if err := voteResultTx.AddInput(wire.OutPoint{Hash: proposalTxID, Index: 1}, agentLockingScript,
		proposalOutputValue); err != nil {
		return nil, errors.Wrap(err, "add input")
	}

	if err := voteResultTx.AddOutput(agentLockingScript, 1, false, false); err != nil {
		return nil, errors.Wrap(err, "add contract output")
	}

	voteResultScript, err := protocol.Serialize(voteResult, config.IsTest)
	if err != nil {
		return nil, errors.Wrap(err, "serialize vote result")
	}

	voteResultScriptOutputIndex := len(voteResultTx.Outputs)
	if err := voteResultTx.AddOutput(voteResultScript, 0, false, false); err != nil {
		return nil, errors.Wrap(err, "add vote result output")
	}

	// Add the contract fee and holder proposal fee.
	contract := a.Contract()
	contract.Lock()
	contractFee := contract.Formation.ContractFee
	contract.Unlock()
	if contractFee > 0 {
		if err := voteResultTx.AddOutput(a.FeeLockingScript(), contractFee, true,
			false); err != nil {
			return nil, errors.Wrap(err, "add contract fee")
		}
	} else if err := voteResultTx.SetChangeLockingScript(a.FeeLockingScript(), ""); err != nil {
		return nil, errors.Wrap(err, "set change")
	}

	// Sign vote tx.
	if _, err := voteResultTx.Sign([]bitcoin.Key{a.Key()}); err != nil {
		return nil, errors.Wrap(err, "sign")
	}

	voteResultTxID := *voteResultTx.MsgTx.TxHash()

	vote.Result = voteResult
	vote.ResultTxID = &voteResultTxID

	voteResultTransaction, err := a.transactions.AddRaw(ctx, voteResultTx.MsgTx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "add response tx")
	}
	defer a.transactions.Release(ctx, voteResultTxID)

	// Set vote result tx as processed since the contract is now formed.
	voteResultTransaction.Lock()
	voteResultTransaction.SetProcessed(a.ContractHash(), voteResultScriptOutputIndex)
	voteResultTransaction.Unlock()

	proposalTransaction.Lock()
	proposalTransaction.AddResponseTxID(a.ContractHash(), outputIndex, voteResultTxID)
	proposalTx := proposalTransaction.Tx.Copy()
	proposalTransaction.Unlock()

	etx, err := buildExpandedTx(voteResultTx.MsgTx, []*wire.MsgTx{proposalTx})
	if err != nil {
		return nil, errors.Wrap(err, "expanded tx")
	}

	if err := postTransactionToContractSubscriptions(ctx, a.caches, agentLockingScript, etx); err != nil {
		return etx, errors.Wrap(err, "post vote result")
	}

	return etx, nil
}
