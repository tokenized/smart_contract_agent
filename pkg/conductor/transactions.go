package conductor

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

func (c *Conductor) addTx(ctx context.Context, txid bitcoin.Hash32,
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
	if !transaction.IsProcessed && transaction.State&wallet.TxStateSafe != 0 {
		if err := c.processTransaction(ctx, transaction); err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "process")
		}

		transaction.IsProcessed = true
		transaction.MarkModified()
		transaction.Unlock()
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

func (c *Conductor) processTransaction(ctx context.Context, transaction TransactionWithOutputs) error {
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

func (c *Conductor) compileTx(ctx context.Context, tx TransactionWithOutputs) (AgentActionsList, error) {
	isTest := c.IsTest()
	var agentActionsList AgentActionsList
	outputCount := tx.OutputCount()
	for index := 0; index < outputCount; index++ {
		output := tx.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		c.compileAction(ctx, &agentActionsList, tx, action)
	}

	return agentActionsList, nil
}

func (c *Conductor) compileAction(ctx context.Context, agentActionsList *AgentActionsList,
	tx TransactionWithOutputs, action actions.Action) error {
	ctx = logger.ContextWithLogFields(ctx, logger.String("action", action.Code()))

	switch act := action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract.
		lockingScript := tx.Output(0).LockingScript

		if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.Reconciliation:

		// Response actions where first input is the contract.
		lockingScript, err := tx.InputLockingScript(0)
		if err != nil {
			return errors.Wrap(err, "input locking script")
		}

		if ra, err := bitcoin.RawAddressFromLockingScript(lockingScript); err == nil {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("address", bitcoin.NewAddressFromRawAddress(ra, bitcoin.MainNet)),
				logger.Stringer("locking_script", lockingScript),
			}, "Contract response found")
		}

		if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
			return errors.Wrap(err, "add agent action")
		}

	case *actions.Transfer:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= tx.OutputCount() {
				return fmt.Errorf("Transfer contract index out of range : %d >= %d",
					instrument.ContractIndex, tx.OutputCount())
			}

			lockingScript := tx.Output(int(instrument.ContractIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Settlement:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= tx.InputCount() {
				return fmt.Errorf("Settlement contract index out of range : %d >= %d",
					instrument.ContractIndex, tx.InputCount())
			}

			lockingScript, err := tx.InputLockingScript(int(instrument.ContractIndex))
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Message:
		for _, senderIndex := range act.SenderIndexes {
			if int(senderIndex) >= tx.InputCount() {
				return fmt.Errorf("Message sender index out of range : %d >= %d", senderIndex,
					tx.InputCount())
			}

			lockingScript, err := tx.InputLockingScript(int(senderIndex))
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

		for _, receiverIndex := range act.ReceiverIndexes {
			if int(receiverIndex) >= tx.OutputCount() {
				return fmt.Errorf("Message receiver index out of range : %d >= %d", receiverIndex,
					tx.OutputCount())
			}

			lockingScript := tx.Output(int(receiverIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	case *actions.Rejection:
		inputCount := tx.InputCount()
		for i := 0; i < inputCount; i++ {
			lockingScript, err := tx.InputLockingScript(i)
			if err != nil {
				return errors.Wrap(err, "input locking script")
			}

			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

		for _, addressIndex := range act.AddressIndexes {
			if int(addressIndex) >= tx.OutputCount() {
				return fmt.Errorf("Reject address index out of range : %d >= %d", addressIndex,
					tx.OutputCount())
			}

			lockingScript := tx.Output(int(addressIndex)).LockingScript
			if err := c.addAgentAction(ctx, agentActionsList, lockingScript, action); err != nil {
				return errors.Wrap(err, "add agent action")
			}
		}

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
