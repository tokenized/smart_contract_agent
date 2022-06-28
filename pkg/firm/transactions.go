package firm

import (
	"context"
	"fmt"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

type TransactionWithOutputs interface {
	TxID() bitcoin.Hash32

	InputCount() int
	Input(index int) *wire.TxIn
	InputLockingScript(index int) (bitcoin.Script, error)

	OutputCount() int
	Output(index int) *wire.TxOut
}

func (f *Firm) addTx(ctx context.Context, txid bitcoin.Hash32,
	spyNodeTx *spynode.Tx) (*state.Transaction, error) {

	transaction := &state.Transaction{
		Tx:      spyNodeTx.Tx,
		Outputs: spyNodeTx.Outputs,
	}

	if spyNodeTx.State.Safe {
		transaction.State = wallet.TxStateSafe
	} else {
		if spyNodeTx.State.UnSafe {
			transaction.State |= wallet.TxStateUnsafe
		}
		if spyNodeTx.State.Cancelled {
			transaction.State |= wallet.TxStateCancelled
		}
	}

	if spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		transaction.MerkleProofs = []*merkle_proof.MerkleProof{mp}
	}

	addedTx, err := f.transactions.Add(ctx, transaction)
	if err != nil {
		return nil, errors.Wrap(err, "add tx")
	}

	if addedTx != transaction && spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		// Transaction already existed, so try to add the merkle proof to it.
		addedTx.Lock()
		if addedTx.AddMerkleProof(mp) {
			addedTx.Unlock()
			if err := f.transactions.Save(ctx, addedTx); err != nil {
				return nil, errors.Wrap(err, "save tx")
			}
		}
		addedTx.Unlock()
	}

	return addedTx, nil
}

func (f *Firm) updateTransaction(ctx context.Context, transaction *state.Transaction) error {
	transaction.Lock()
	if !transaction.IsProcessed && transaction.State&wallet.TxStateSafe != 0 {
		if err := f.processTransaction(ctx, transaction); err != nil {
			return errors.Wrap(err, "process")
		}

		transaction.IsProcessed = true
		transaction.Unlock()

		if err := f.transactions.Save(ctx, transaction); err != nil {
			return errors.Wrap(err, "save tx")
		}

		return nil
	}

	if transaction.IsProcessed && (transaction.State&wallet.TxStateUnsafe != 0 ||
		transaction.State&wallet.TxStateCancelled != 0) {
		logger.Error(ctx, "Processed transaction is unsafe")

		// TODO Perform actions to resolve unsafe or double spent tx. --ce
	}
	transaction.Unlock()

	return nil
}

func (f *Firm) processTransaction(ctx context.Context, transaction TransactionWithOutputs) error {
	agentActionsList, err := f.compileTx(ctx, transaction)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(agentActionsList) == 0 {
		logger.Warn(ctx, "Transaction not relevant")
		return nil
	}
	defer f.releaseAgentActions(ctx, agentActionsList)

	for _, agentActions := range agentActionsList {
		if err := agentActions.agent.Process(ctx, transaction, agentActions.actions); err != nil {
			var codes []string
			for _, action := range agentActions.actions {
				codes = append(codes, action.Code())
			}

			logger.ErrorWithFields(ctx, []logger.Field{
				logger.Stringer("agent", agentActions.agent.LockingScript()),
				logger.Strings("actions", codes),
			}, "Agent failed to handle transaction : %s", err)
		}
	}

	return nil
}

func (f *Firm) compileTx(ctx context.Context, tx TransactionWithOutputs) (AgentActionsList, error) {
	isTest := f.IsTest()
	var agentActionsList AgentActionsList
	outputCount := tx.OutputCount()
	for index := 0; index < outputCount; index++ {
		output := tx.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		f.compileAction(ctx, &agentActionsList, tx, action)
	}

	return agentActionsList, nil
}

func (f *Firm) compileAction(ctx context.Context, agentActionsList *AgentActionsList,
	tx TransactionWithOutputs, action actions.Action) error {
	ctx = logger.ContextWithLogFields(ctx, logger.String("action", action.Code()))

	switch act := action.(type) {
	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.Reconciliation:

		// Response actions where first input is the contract.
		lockingScript, err := tx.InputLockingScript(0)
		if err != nil {
			return errors.Wrap(err, "input locking script")
		}

		if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract.
		lockingScript := tx.Output(0).LockingScript

		if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.Transfer:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= tx.InputCount() {
				return fmt.Errorf("Transfer contract index out of range : %d >= %d",
					instrument.ContractIndex, tx.InputCount())
			}

			lockingScript, err := tx.InputLockingScript(int(instrument.ContractIndex))
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Settlement:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= tx.OutputCount() {
				return fmt.Errorf("Settlement contract index out of range : %d >= %d",
					instrument.ContractIndex, tx.OutputCount())
			}

			lockingScript := tx.Output(int(instrument.ContractIndex)).LockingScript

			if err := f.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	// case *actions.Message:
	// case *actions.Rejection:

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
