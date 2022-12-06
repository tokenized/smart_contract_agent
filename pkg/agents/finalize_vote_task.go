package agents

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// FinalizeVoteTask executes when the specified cut off time for the proposal is reached, then
// calculates the result of the vote by counting all the ballots and broadcasts the Result action.
type FinalizeVoteTask struct {
	start time.Time

	factory               AgentFactory
	contractLockingScript bitcoin.Script
	voteTxID              bitcoin.Hash32
	timestamp             uint64

	lock sync.Mutex
}

func NewFinalizeVoteTask(start time.Time, factory AgentFactory,
	contractLockingScript bitcoin.Script, voteTxID bitcoin.Hash32,
	timestamp uint64) *FinalizeVoteTask {

	return &FinalizeVoteTask{
		start:                 start,
		factory:               factory,
		contractLockingScript: contractLockingScript,
		voteTxID:              voteTxID,
		timestamp:             timestamp,
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

func (t *FinalizeVoteTask) Run(ctx context.Context, interrupt <-chan interface{}) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	factory := t.factory
	contractLockingScript := t.contractLockingScript
	voteTxID := t.voteTxID
	timestamp := t.timestamp

	return FinalizeVote(ctx, factory, contractLockingScript, voteTxID, timestamp)
}

func FinalizeVote(ctx context.Context, factory AgentFactory, contractLockingScript bitcoin.Script,
	voteTxID bitcoin.Hash32, now uint64) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("vote_txid", voteTxID),
		logger.Stringer("contract_locking_script", contractLockingScript),
	}, "Finalizing vote")

	agent, err := factory.GetAgent(ctx, contractLockingScript)
	if err != nil {
		return errors.Wrap(err, "get agent")
	}

	if agent == nil {
		return errors.New("Agent not found")
	}
	defer agent.Release(ctx)

	return agent.FinalizeVote(ctx, voteTxID, now)
}

func (a *Agent) FinalizeVote(ctx context.Context, voteTxID bitcoin.Hash32, now uint64) error {
	agentLockingScript := a.LockingScript()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("vote_txid", voteTxID),
		logger.Stringer("contract_locking_script", agentLockingScript))

	logger.Info(ctx, "Finalizing vote")

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

	isTest := a.IsTest()
	proposalOutputCount := proposalTransaction.OutputCount()
	outputIndex := 0
	for i := 0; i < proposalOutputCount; i++ {
		output := proposalTransaction.Output(i)
		action, err := protocol.Deserialize(output.LockingScript, isTest)
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
		Timestamp:          now,
	}

	if err := voteResult.Validate(); err != nil {
		return errors.Wrap(a.sendRejection(ctx, proposalTransaction, 1,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now), "reject")
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

	voteResultScriptOutputIndex := len(voteResultTx.Outputs)
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
	voteResultTransaction.SetProcessed(a.ContractHash(), voteResultScriptOutputIndex)
	voteResultTransaction.Unlock()

	proposalTransaction.Lock()
	proposalTransaction.AddResponseTxID(a.ContractHash(), outputIndex, voteResultTxID)
	proposalTx := proposalTransaction.Tx.Copy()
	proposalTransaction.Unlock()

	etx, err := buildExpandedTx(voteResultTx.MsgTx, []*wire.MsgTx{proposalTx})
	if err != nil {
		return errors.Wrap(err, "expanded tx")
	}

	if err := a.BroadcastTx(ctx, etx, nil); err != nil {
		return errors.Wrap(err, "broadcast")
	}

	if err := a.postTransactionToContractSubscriptions(ctx, voteResultTransaction); err != nil {
		return errors.Wrap(err, "post vote result")
	}

	return nil
}
