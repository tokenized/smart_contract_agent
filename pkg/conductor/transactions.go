package conductor

import (
	"context"
	"fmt"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	spynode "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

func (c *Conductor) addTx(ctx context.Context, txid bitcoin.Hash32,
	spyNodeTx *spynode.Tx) (*state.Transaction, error) {

	transaction := &state.Transaction{
		Tx: spyNodeTx.Tx,
	}

	transaction.SpentOutputs = make([]*expanded_tx.Output, len(spyNodeTx.Outputs))
	for i, txout := range spyNodeTx.Outputs {
		transaction.SpentOutputs[i] = &expanded_tx.Output{
			Value:         txout.Value,
			LockingScript: txout.LockingScript,
		}
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

	addedTx, err := c.transactions.Add(ctx, transaction)
	if err != nil {
		return nil, errors.Wrap(err, "add tx")
	}

	if addedTx != transaction && spyNodeTx.State.MerkleProof != nil {
		mp := spyNodeTx.State.MerkleProof.ConvertToMerkleProof(txid)
		// Transaction already existed, so try to add the merkle proof to it.
		addedTx.Lock()
		addedTx.AddMerkleProof(mp)
		addedTx.Unlock()
	}

	return addedTx, nil
}

func (c *Conductor) UpdateTransaction(ctx context.Context, transaction *state.Transaction) error {
	transaction.Lock()
	isProcessed := transaction.IsProcessed
	txState := transaction.State
	transaction.Unlock()

	if !isProcessed && txState&wallet.TxStateSafe != 0 {
		if err := c.processTransaction(ctx, transaction); err != nil {
			return errors.Wrap(err, "process")
		}

		transaction.Lock()
		transaction.IsProcessed = true
		transaction.MarkModified()
		transaction.Unlock()
		return nil
	}

	if isProcessed && (txState&wallet.TxStateUnsafe != 0 || txState&wallet.TxStateCancelled != 0) {
		logger.Error(ctx, "Processed transaction is unsafe")

		// TODO Perform actions to resolve unsafe or double spent tx. --ce
	}

	return nil
}

func (c *Conductor) processTransaction(ctx context.Context, transaction *state.Transaction) error {
	agentActionsList, err := c.compileTx(ctx, transaction)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(agentActionsList) == 0 {
		logger.Warn(ctx, "Transaction not relevant")
		return nil
	}
	defer c.releaseAgentActions(ctx, agentActionsList)

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

func (c *Conductor) compileTx(ctx context.Context,
	transaction *state.Transaction) (AgentActionsList, error) {

	isTest := c.IsTest()
	var agentActionsList AgentActionsList

	transaction.Lock()
	outputCount := transaction.OutputCount()
	outputs := make([]*wire.TxOut, outputCount)
	for index := 0; index < outputCount; index++ {
		outputs[index] = transaction.Output(index)
	}
	transaction.Unlock()

	for _, output := range outputs {
		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		c.compileAction(ctx, &agentActionsList, transaction, action)
	}

	return agentActionsList, nil
}

func (c *Conductor) compileAction(ctx context.Context, agentActionsList *AgentActionsList,
	transaction *state.Transaction, action actions.Action) error {
	ctx = logger.ContextWithLogFields(ctx, logger.String("action", action.Code()))

	transaction.Lock()
	defer transaction.Unlock()

	switch act := action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract.
		lockingScript := transaction.Output(0).LockingScript

		if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.Reconciliation:

		// Response actions where first input is the contract.
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return errors.Wrap(err, "input locking script")
		}

		if ra, err := bitcoin.RawAddressFromLockingScript(inputOutput.LockingScript); err == nil {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("address", bitcoin.NewAddressFromRawAddress(ra, bitcoin.MainNet)),
				logger.Stringer("locking_script", inputOutput.LockingScript),
			}, "Contract response found")
		}

		if err := c.addAgentAction(ctx, agentActionsList, inputOutput.LockingScript,
			action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.Transfer:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.OutputCount() {
				return fmt.Errorf("Transfer contract index out of range : %d >= %d",
					instrument.ContractIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(instrument.ContractIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Settlement:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.InputCount() {
				return fmt.Errorf("Settlement contract index out of range : %d >= %d",
					instrument.ContractIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, inputOutput.LockingScript,
				action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Message:
		for _, senderIndex := range act.SenderIndexes {
			if int(senderIndex) >= transaction.InputCount() {
				return fmt.Errorf("Message sender index out of range : %d >= %d", senderIndex,
					transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(senderIndex))
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, inputOutput.LockingScript,
				action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

		for _, receiverIndex := range act.ReceiverIndexes {
			if int(receiverIndex) >= transaction.OutputCount() {
				return fmt.Errorf("Message receiver index out of range : %d >= %d", receiverIndex,
					transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(receiverIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Rejection:
		inputCount := transaction.InputCount()
		for i := 0; i < inputCount; i++ {
			inputOutput, err := transaction.InputOutput(i)
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, inputOutput.LockingScript,
				action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

		for _, addressIndex := range act.AddressIndexes {
			if int(addressIndex) >= transaction.OutputCount() {
				return fmt.Errorf("Reject address index out of range : %d >= %d", addressIndex,
					transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(addressIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
