package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type RecoveryRequest struct {
	TxID          bitcoin.Hash32 `bsor:"1" json:"txid"`
	OutputIndexes []int          `bsor:"2" json:"output_indexes"`
}

type Action struct {
	OutputIndex int
	Action      actions.Action
}

func (a *Agent) UpdateTransaction(ctx context.Context, transaction *state.Transaction) error {
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

		if err := a.processTransaction(ctx, transaction); err != nil {
			return errors.Wrap(err, "process")
		}

		return nil
	}

	if txState&wallet.TxStateUnsafe != 0 || txState&wallet.TxStateCancelled != 0 {
		transaction.Lock()
		if !transaction.SetIsProcessing(contractHash) {
			transaction.Unlock()
			return nil
		}
		transaction.Unlock()
		defer clearIsProcessing(transaction, contractHash)

		if err := a.processUnsafeTransaction(ctx, transaction); err != nil {
			return errors.Wrap(err, "process unsafe")
		}
	}

	return nil
}

func (a *Agent) Process(ctx context.Context, transaction *state.Transaction,
	actionList []Action) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()

	var requestActions []Action
	var responseActions []Action
	for _, action := range actionList {
		if isRequest(action.Action) {
			requestActions = append(requestActions, action)
		} else {
			responseActions = append(responseActions, action)
		}
	}

	if inRecovery, err := a.addRecoveryRequests(ctx, txid, requestActions); err != nil {
		return errors.Wrap(err, "recovery request")
	} else if inRecovery {
		var actionCodes []string
		for _, a := range requestActions {
			actionCodes = append(actionCodes, a.Action.Code())
		}
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Stringer("contract_locking_script", agentLockingScript),
			logger.Strings("actions", actionCodes),
		}, "Saving transaction requests for recovery")

		if len(responseActions) == 0 {
			return nil
		}

		// Process only the responses
		actionList = responseActions
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing transaction")

	var feeRate, minFeeRate float32
	if len(requestActions) > 0 {
		transaction.Lock()
		fee, err := transaction.CalculateFee()
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "calculate fee")
		}
		size := transaction.Size()
		transaction.Unlock()

		minFeeRate = a.MinFeeRate()
		feeRate = float32(fee) / float32(size)
	}

	for i, action := range actionList {
		if isRequest(action.Action) {
			if feeRate < minFeeRate {
				etx, err := a.createRejection(ctx, transaction, action.OutputIndex,
					platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
						fmt.Sprintf("fee rate %.4f, minimum %.4f", feeRate, minFeeRate)))
				if err != nil {
					return errors.Wrap(err, "create rejection")
				}

				if etx != nil {
					if err := a.BroadcastTx(ctx, etx, nil); err != nil {
						return errors.Wrap(err, "broadcast")
					}
				}
			}
		}

		if err := a.processAction(ctx, transaction, txid, action.Action,
			action.OutputIndex); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, transaction *state.Transaction,
	txid bitcoin.Hash32, action actions.Action, outputIndex int) error {
	start := time.Now()

	processed := transaction.ContractProcessed(a.ContractHash(), outputIndex)
	if len(processed) > 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Int("output_index", outputIndex),
			logger.String("action_code", action.Code()),
			logger.String("action_name", action.TypeName()),
			logger.Stringer("response_txid", processed[0].ResponseTxID),
		}, "Action already processed")
		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Int("output_index", outputIndex),
		logger.String("action_code", action.Code()),
		logger.String("action_name", action.TypeName()),
	}, "Processing action")

	isRequest := isRequest(action)

	if err := action.Validate(); err != nil {
		if isRequest {
			etx, err := a.createRejection(ctx, transaction, outputIndex,
				platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()))
			if err != nil {
				return errors.Wrap(err, "create rejection")
			}

			if etx != nil {
				if err := a.BroadcastTx(ctx, etx, nil); err != nil {
					return errors.Wrap(err, "broadcast")
				}
			}
		} else {
			logger.ErrorWithFields(ctx, []logger.Field{
				logger.Stringer("txid", txid),
				logger.Int("output_index", outputIndex),
				logger.String("action_code", action.Code()),
				logger.String("action_name", action.TypeName()),
			}, "Response action invalid : %s", err)
		}
	}

	var responseEtx *expanded_tx.ExpandedTx
	var processError error
	switch act := action.(type) {
	case *actions.ContractOffer:
		responseEtx, processError = a.processContractOffer(ctx, transaction, act, outputIndex)

	case *actions.ContractAmendment:
		responseEtx, processError = a.processContractAmendment(ctx, transaction, act, outputIndex)

	case *actions.ContractFormation:
		processError = a.processContractFormation(ctx, transaction, act, outputIndex)

	case *actions.ContractAddressChange:
		responseEtx, processError = a.processContractAddressChange(ctx, transaction, act,
			outputIndex)

	case *actions.BodyOfAgreementOffer:
		responseEtx, processError = a.processBodyOfAgreementOffer(ctx, transaction, act,
			outputIndex)

	case *actions.BodyOfAgreementAmendment:
		responseEtx, processError = a.processBodyOfAgreementAmendment(ctx, transaction, act,
			outputIndex)

	case *actions.BodyOfAgreementFormation:
		processError = a.processBodyOfAgreementFormation(ctx, transaction, act, outputIndex)

	case *actions.InstrumentDefinition:
		responseEtx, processError = a.processInstrumentDefinition(ctx, transaction, act,
			outputIndex)

	case *actions.InstrumentModification:
		responseEtx, processError = a.processInstrumentModification(ctx, transaction, act,
			outputIndex)

	case *actions.InstrumentCreation:
		processError = a.processInstrumentCreation(ctx, transaction, act, outputIndex)

	case *actions.Transfer:
		responseEtx, processError = a.processTransfer(ctx, transaction, act, outputIndex)

	case *actions.Settlement:
		processError = a.processSettlement(ctx, transaction, act, outputIndex)

	case *actions.RectificationSettlement:
		// TODO Create function that watches for "double spent" requests and sends Rectification
		// Settlements. --ce
		processError = a.processRectificationSettlement(ctx, transaction, act, outputIndex)

	case *actions.Proposal:
		responseEtx, processError = a.processProposal(ctx, transaction, act, outputIndex)

	case *actions.Vote:
		processError = a.processVote(ctx, transaction, act, outputIndex)

	case *actions.BallotCast:
		responseEtx, processError = a.processBallotCast(ctx, transaction, act, outputIndex)

	case *actions.BallotCounted:
		processError = a.processBallotCounted(ctx, transaction, act, outputIndex)

	case *actions.Result:
		processError = a.processVoteResult(ctx, transaction, act, outputIndex)

	case *actions.Order:
		responseEtx, processError = a.processOrder(ctx, transaction, act, outputIndex)

	case *actions.Freeze:
		processError = a.processFreeze(ctx, transaction, act, outputIndex)

	case *actions.Thaw:
		processError = a.processThaw(ctx, transaction, act, outputIndex)

	case *actions.Confiscation:
		processError = a.processConfiscation(ctx, transaction, act, outputIndex)

	case *actions.DeprecatedReconciliation:
		processError = a.processReconciliation(ctx, transaction, act, outputIndex)

	case *actions.Message:
		responseEtx, processError = a.processMessage(ctx, transaction, act, outputIndex)

	case *actions.Rejection:
		responseEtx, processError = a.processRejection(ctx, transaction, act, outputIndex)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		logger.Stringer("txid", txid),
		logger.Int("output_index", outputIndex),
		logger.String("action_code", action.Code()),
		logger.String("action_name", action.TypeName()),
	}, "Processed action")

	if responseEtx != nil {
		if err := a.BroadcastTx(ctx, responseEtx, nil); err != nil {
			return errors.Wrap(err, "broadcast")
		}
	}

	if processError != nil {
		if rejectError, ok := errors.Cause(processError).(platform.RejectError); ok {
			etx, err := a.createRejection(ctx, transaction, outputIndex, rejectError)
			if err != nil {
				return errors.Wrap(err, "create rejection")
			}

			if etx != nil {
				if err := a.BroadcastTx(ctx, etx, nil); err != nil {
					return errors.Wrap(err, "broadcast")
				}
			}

			return nil
		}

		return errors.Wrap(processError, "process")
	}

	return nil
}

// ProcessUnsafe performs actions to resolve unsafe or double spent tx.
func (a *Agent) ProcessUnsafe(ctx context.Context, transaction *state.Transaction,
	actionList []Action) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()
	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing unsafe transaction")

	for i, action := range actionList {
		if isRequest(action.Action) {
			processed := transaction.ContractProcessed(a.ContractHash(), action.OutputIndex)
			if len(processed) > 0 {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("txid", txid),
					logger.Int("output_index", action.OutputIndex),
					logger.String("action_code", action.Action.Code()),
					logger.String("action_name", action.Action.TypeName()),
					logger.Stringer("response_txid", processed[0].ResponseTxID),
				}, "Unsafe action already processed")
				return nil
			}

			etx, err := a.createRejection(ctx, transaction, action.OutputIndex,
				platform.NewRejectError(actions.RejectionsDoubleSpend, ""))
			if err != nil {
				return errors.Wrap(err, "create rejection")
			}

			if etx != nil {
				if err := a.BroadcastTx(ctx, etx, nil); err != nil {
					return errors.Wrap(err, "broadcast")
				}
			}
		}

		// If it isn't a request then we can process it like normal.
		if err := a.processAction(ctx, transaction, txid, action.Action,
			action.OutputIndex); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}

func clearIsProcessing(transaction *state.Transaction, contract state.ContractHash) {
	transaction.Lock()
	transaction.ClearIsProcessing(contract)
	transaction.Unlock()
}

func (a *Agent) processUnsafeTransaction(ctx context.Context,
	transaction *state.Transaction) error {

	txid := transaction.GetTxID()

	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
	}, "Processing transaction is unsafe")

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()

	actionsList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(actionsList) == 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
		}, "Transaction not relevant")
		return nil
	}

	if err := a.ProcessUnsafe(ctx, transaction, actionsList); err != nil {
		var codes []string
		for _, action := range actionsList {
			codes = append(codes, action.Action.Code())
		}

		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Stringer("agent", agentLockingScript),
			logger.Strings("actions", codes),
		}, "Agent failed to handle transaction : %s", err)
	}

	return nil

}

func (a *Agent) processTransaction(ctx context.Context, transaction *state.Transaction) error {
	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()

	actionsList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	if len(actionsList) == 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", transaction.GetTxID()),
		}, "Transaction not relevant")
		return nil
	}

	if err := a.Process(ctx, transaction, actionsList); err != nil {
		var codes []string
		for _, action := range actionsList {
			codes = append(codes, action.Action.Code())
		}

		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("txid", transaction.GetTxID()),
			logger.Stringer("agent", agentLockingScript),
			logger.Strings("actions", codes),
		}, "Agent failed to handle transaction : %s", err)
	}

	return nil
}

func relevantRequestOutputs(etx *expanded_tx.ExpandedTx, agentLockingScript bitcoin.Script,
	isTest bool) ([]int, error) {

	var result []int
	outputCount := etx.OutputCount()
	for index := 0; index < outputCount; index++ {
		output := etx.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if !isRequest(action) {
			continue
		}

		isRelevant, err := ActionIsRelevent(etx, action, agentLockingScript)
		if err != nil {
			return nil, errors.Wrap(err, "relevant")
		}

		if isRelevant {
			result = append(result, index)
		}
	}

	return result, nil
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

		isRelevant, err := ActionIsRelevent(transaction, action, agentLockingScript)
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

// ActionIsRelevent returns true if the action is a request to the agent locking script, or a
// response from the agent locking script.
func ActionIsRelevent(transaction expanded_tx.TransactionWithOutputs, action actions.Action,
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
