package agents

import (
	"context"
	"fmt"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

type Action struct {
	OutputIndex int
	Action      actions.Action
}

func (a *Agent) UpdateTransaction(ctx context.Context, transaction *state.Transaction,
	now uint64) error {

	contractHash := a.ContractHash()

	transaction.Lock()
	txState := transaction.State
	transaction.Unlock()

	if txState&wallet.TxStateSafe != 0 {
		transaction.Lock()
		if !transaction.SetIsProcessing(contractHash) {
			transaction.Unlock()
			return nil
		}
		transaction.Unlock()
		defer clearIsProcessing(transaction, contractHash)

		if err := a.processTransaction(ctx, transaction, now); err != nil {
			return errors.Wrap(err, "process")
		}

		return nil
	}

	if txState&wallet.TxStateUnsafe != 0 || txState&wallet.TxStateCancelled != 0 {
		logger.Error(ctx, "Processed transaction is unsafe")

		if err := a.processUnsafeTransaction(ctx, transaction, now); err != nil {
			return errors.Wrap(err, "process unsafe")
		}

	}

	return nil
}

func clearIsProcessing(transaction *state.Transaction, contract state.ContractHash) {
	transaction.Lock()
	transaction.ClearIsProcessing(contract)
	transaction.Unlock()
}

func (a *Agent) processUnsafeTransaction(ctx context.Context, transaction *state.Transaction,
	now uint64) error {

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()

	actionsList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(actionsList) == 0 {
		logger.Info(ctx, "Transaction not relevant")
		return nil
	}

	if err := a.ProcessUnsafe(ctx, transaction, actionsList, now); err != nil {
		var codes []string
		for _, action := range actionsList {
			codes = append(codes, action.Action.Code())
		}

		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("agent", agentLockingScript),
			logger.Strings("actions", codes),
		}, "Agent failed to handle transaction : %s", err)
	}

	return nil

}

func (a *Agent) processTransaction(ctx context.Context, transaction *state.Transaction,
	now uint64) error {

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()

	actionsList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(actionsList) == 0 {
		logger.Info(ctx, "Transaction not relevant")
		return nil
	}

	if err := a.Process(ctx, transaction, actionsList, now); err != nil {
		var codes []string
		for _, action := range actionsList {
			codes = append(codes, action.Action.Code())
		}

		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("agent", agentLockingScript),
			logger.Strings("actions", codes),
		}, "Agent failed to handle transaction : %s", err)
	}

	return nil
}

func compileActions(transaction *state.Transaction, agentLockingScript bitcoin.Script,
	isTest bool) ([]Action, error) {

	var result []Action

	transaction.Lock()
	defer transaction.Unlock()

	outputCount := transaction.OutputCount()

	for index := 0; index < outputCount; index++ {
		output := transaction.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		isRelevant, err := actionIsRelevent(transaction, action, agentLockingScript)
		if err != nil {
			return nil, errors.Wrap(err, "relevant")
		}

		if isRelevant {
			result = append(result, Action{
				OutputIndex: index,
				Action:      action,
			})
		}
	}

	return result, nil
}

func actionIsRelevent(transaction *state.Transaction, action actions.Action,
	agentLockingScript bitcoin.Script) (bool, error) {

	switch act := action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract.
		lockingScript := transaction.Output(0).LockingScript

		return agentLockingScript.Equal(lockingScript), nil

	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.DeprecatedReconciliation:

		// Response actions where first input is the contract.
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return false, errors.Wrap(err, "input locking script")
		}

		return agentLockingScript.Equal(inputOutput.LockingScript), nil

	case *actions.Transfer:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.OutputCount() {
				return false, fmt.Errorf("Transfer contract index out of range : %d >= %d",
					instrument.ContractIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(instrument.ContractIndex)).LockingScript

			if agentLockingScript.Equal(lockingScript) {
				return true, nil
			}
		}

		return false, nil

	case *actions.Settlement:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.InputCount() {
				return false, fmt.Errorf("Settlement contract index out of range : %d >= %d",
					instrument.ContractIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return false, errors.Wrap(err, "input locking script")
			}

			if agentLockingScript.Equal(inputOutput.LockingScript) {
				return true, nil
			}
		}

		return false, nil

	case *actions.RectificationSettlement:
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.InputCount() {
				return false, fmt.Errorf("Settlement contract index out of range : %d >= %d",
					instrument.ContractIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return false, errors.Wrap(err, "input locking script")
			}

			if agentLockingScript.Equal(inputOutput.LockingScript) {
				return true, nil
			}
		}

		return false, nil

	case *actions.Message:
		for _, senderIndex := range act.SenderIndexes {
			if int(senderIndex) >= transaction.InputCount() {
				return false, fmt.Errorf("Message sender index out of range : %d >= %d",
					senderIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(senderIndex))
			if err != nil {
				return false, errors.Wrap(err, "input locking script")
			}

			if agentLockingScript.Equal(inputOutput.LockingScript) {
				return true, nil
			}
		}

		for _, receiverIndex := range act.ReceiverIndexes {
			if int(receiverIndex) >= transaction.OutputCount() {
				return false, fmt.Errorf("Message receiver index out of range : %d >= %d",
					receiverIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(receiverIndex)).LockingScript

			if agentLockingScript.Equal(lockingScript) {
				return true, nil
			}
		}

		return false, nil

	case *actions.Rejection:
		inputCount := transaction.InputCount()
		for i := 0; i < inputCount; i++ {
			inputOutput, err := transaction.InputOutput(i)
			if err != nil {
				return false, errors.Wrap(err, "input locking script")
			}

			if agentLockingScript.Equal(inputOutput.LockingScript) {
				return true, nil
			}
		}

		for _, addressIndex := range act.AddressIndexes {
			if int(addressIndex) >= transaction.OutputCount() {
				return false, fmt.Errorf("Reject address index out of range : %d >= %d",
					addressIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(addressIndex)).LockingScript

			if agentLockingScript.Equal(lockingScript) {
				return true, nil
			}
		}

		return false, nil

	default:
		return false, fmt.Errorf("Action not supported: %s", action.Code())
	}

	return false, nil
}
