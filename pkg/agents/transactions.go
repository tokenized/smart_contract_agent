package agents

import (
	"context"
	"fmt"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ErrInvalidAction = errors.New("Invalid Action")
)

type RecoveryRequest struct {
	TxID          bitcoin.Hash32 `bsor:"1" json:"txid"`
	OutputIndexes []int          `bsor:"2" json:"output_indexes"`
}

type Action struct {
	AgentLockingScripts []bitcoin.Script
	OutputIndex         int
	Action              actions.Action
}

func (a Action) IsRelevant(agentLockingScript bitcoin.Script) bool {
	for _, ls := range a.AgentLockingScripts {
		if ls.Equal(agentLockingScript) {
			return true
		}
	}

	return false
}

func compileActions(ctx context.Context, transaction *transactions.Transaction,
	isTest bool) ([]Action, error) {

	transaction.Lock()
	defer transaction.Unlock()

	return CompileActions(ctx, transaction, isTest)
}

func CompileActions(ctx context.Context, transaction expanded_tx.TransactionWithOutputs,
	isTest bool) ([]Action, error) {

	outputCount := transaction.OutputCount()

	var result []Action
	for index := 0; index < outputCount; index++ {
		output := transaction.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			if errors.Cause(err) == protocol.ErrNotTokenized {
				continue
			}

			return nil, errors.Wrapf(errors.Wrap(ErrInvalidAction, err.Error()), "output %d", index)
		}

		agentLockingScripts, err := ReleventAgentLockingScripts(transaction, action)
		if err != nil {
			return nil, errors.Wrapf(err, "output %d", index)
		}

		if len(agentLockingScripts) == 0 {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", transaction.TxID()),
				logger.String("action_code", action.Code()),
				logger.Int("output_index", index),
			}, "No relevant agent locking scripts")
			continue
		}

		result = append(result, Action{
			AgentLockingScripts: agentLockingScripts,
			OutputIndex:         index,
			Action:              action,
		})
	}

	return result, nil
}

// ReleventAgentLockingScripts returns the agent locking scripts that are relevant to this action.
func ReleventAgentLockingScripts(transaction expanded_tx.TransactionWithOutputs,
	action actions.Action) ([]bitcoin.Script, error) {

	switch act := action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract agent.
		return []bitcoin.Script{transaction.Output(0).LockingScript}, nil

	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.DeprecatedReconciliation:

		// Response actions where first input is the contract agent.
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return nil, errors.Wrap(err, "input locking script")
		}

		return []bitcoin.Script{inputOutput.LockingScript}, nil

	case *actions.Transfer:
		var result []bitcoin.Script
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.OutputCount() {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(instrument.ContractIndex)).LockingScript
			result = appendLockingScript(result, lockingScript)
		}

		return result, nil

	case *actions.Settlement:
		var result []bitcoin.Script
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.InputCount() {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = appendLockingScript(result, inputOutput.LockingScript)
		}

		return result, nil

	case *actions.RectificationSettlement:
		var result []bitcoin.Script
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= transaction.InputCount() {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = appendLockingScript(result, inputOutput.LockingScript)
		}

		return result, nil

	case *actions.Message:
		var result []bitcoin.Script
		for _, senderIndex := range act.SenderIndexes {
			if int(senderIndex) >= transaction.InputCount() {
				return nil, errors.Wrapf(ErrInvalidAction, "sender index out of range : %d >= %d",
					senderIndex, transaction.InputCount())
			}

			inputOutput, err := transaction.InputOutput(int(senderIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = appendLockingScript(result, inputOutput.LockingScript)
		}

		for _, receiverIndex := range act.ReceiverIndexes {
			if int(receiverIndex) >= transaction.OutputCount() {
				return nil, errors.Wrapf(ErrInvalidAction, "receiver index out of range : %d >= %d",
					receiverIndex, transaction.OutputCount())
			}

			lockingScript := transaction.Output(int(receiverIndex)).LockingScript
			result = appendLockingScript(result, lockingScript)
		}

		return result, nil

	case *actions.Rejection:
		var result []bitcoin.Script

		// First input
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return nil, errors.Wrap(err, "input locking script")
		}
		result = appendLockingScript(result, inputOutput.LockingScript)

		outputCount := transaction.OutputCount()
		if int(act.RejectAddressIndex) >= outputCount {
			return nil, errors.Wrapf(ErrInvalidAction, "reject index out of range : %d >= %d",
				int(act.RejectAddressIndex), transaction.OutputCount())
		}

		lockingScript := transaction.Output(int(act.RejectAddressIndex)).LockingScript
		result = appendLockingScript(result, lockingScript)

		if len(act.AddressIndexes) == 0 {
			// First output
			lockingScript := transaction.Output(0).LockingScript
			result = appendLockingScript(result, lockingScript)
		} else {
			for _, addressIndex := range act.AddressIndexes {
				if int(addressIndex) >= outputCount {
					return nil, errors.Wrapf(ErrInvalidAction, "address index out of range : %d >= %d",
						addressIndex, transaction.OutputCount())
				}

				lockingScript := transaction.Output(int(addressIndex)).LockingScript
				result = appendLockingScript(result, lockingScript)
			}
		}

		return result, nil

	default:
		return nil, fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil, nil
}

func (a *Agent) UpdateTransaction(ctx context.Context, transaction *transactions.Transaction,
	actionList []Action) error {
	contractHash := a.ContractHash()

	transaction.Lock()
	txState := transaction.State
	transaction.Unlock()

	if txState&transactions.TxStateSafe != 0 {
		transaction.Lock()
		if !transaction.SetIsProcessing(contractHash) {
			transaction.Unlock()
			return nil
		}
		transaction.Unlock()
		defer clearIsProcessing(transaction, contractHash)

		if err := a.Process(ctx, transaction, actionList); err != nil {
			return errors.Wrap(err, "process")
		}

		return nil
	}

	if txState&transactions.TxStateUnsafe != 0 || txState&transactions.TxStateCancelled != 0 {
		transaction.Lock()
		if !transaction.SetIsProcessing(contractHash) {
			transaction.Unlock()
			return nil
		}
		transaction.Unlock()
		defer clearIsProcessing(transaction, contractHash)

		if err := a.ProcessUnsafe(ctx, transaction, actionList); err != nil {
			return errors.Wrap(err, "process unsafe")
		}
	}

	return nil
}

func clearIsProcessing(transaction *transactions.Transaction, contract state.ContractHash) {
	transaction.Lock()
	transaction.ClearIsProcessing(contract)
	transaction.Unlock()
}

func (a *Agent) Process(ctx context.Context, transaction *transactions.Transaction,
	actionList []Action) error {

	trace := uuid.New()
	txCtx := logger.ContextWithLogFields(ctx, logger.Stringer("tx_trace", trace))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()

	var relevantActions []Action
	containsRequest := false
	for _, action := range actionList {
		if !action.IsRelevant(agentLockingScript) {
			continue
		}

		relevantActions = append(relevantActions, action)

		if !containsRequest && IsRequest(action.Action) {
			containsRequest = true
		}
	}

	logger.InfoWithFields(txCtx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing transaction")

	config := a.Config()
	minFeeRate := config.MinFeeRate
	var feeRate float64
	if containsRequest {
		transaction.Lock()
		fee, err := transaction.CalculateFee()
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "calculate fee")
		}
		size := transaction.Size()
		transaction.Unlock()

		feeRate = float64(fee) / float64(size)
	}

	for i, action := range relevantActions {
		if err := a.processAction(ctx, agentLockingScript, transaction, txid, action.Action,
			action.OutputIndex, config.RecoveryMode, feeRate, minFeeRate, trace); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}

// ProcessUnsafe performs actions to resolve unsafe or double spent tx.
func (a *Agent) ProcessUnsafe(ctx context.Context, transaction *transactions.Transaction,
	actionList []Action) error {

	trace := uuid.New()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("tx_trace", trace))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()
	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing unsafe transaction")

	for i, action := range actionList {
		if IsRequest(action.Action) {
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

			etx, err := a.createRejection(ctx, transaction, action.OutputIndex, -1,
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
		if err := a.processAction(ctx, agentLockingScript, transaction, txid, action.Action,
			action.OutputIndex, false, 1.0, 0.0, trace); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, agentLockingScript bitcoin.Script,
	transaction *transactions.Transaction, txid bitcoin.Hash32, action actions.Action,
	outputIndex int, inRecovery bool, feeRate, minFeeRate float64, trace uuid.UUID) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("action_trace", uuid.New()))
	start := time.Now()

	processed := transaction.ContractProcessed(a.ContractHash(), outputIndex)
	if len(processed) > 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("tx_trace", trace), // link tx processing to action processing
			logger.Stringer("contract_locking_script", agentLockingScript),
			logger.Stringer("txid", txid),
			logger.Int("output_index", outputIndex),
			logger.String("action_code", action.Code()),
			logger.String("action_name", action.TypeName()),
			logger.Stringer("response_txid", processed[0].ResponseTxID),
		}, "Action already processed")
		return nil
	}

	if inRecovery && IsRequest(action) {
		if added, err := a.addRecoveryRequest(ctx, agentLockingScript, txid,
			outputIndex); err != nil {
			return errors.Wrap(err, "recovery request")
		} else if added {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("tx_trace", trace), // link tx processing to action processing
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", txid),
				logger.String("action", action.Code()),
				logger.Int("output_index", outputIndex),
			}, "Saved transaction request for recovery")
		} else {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("tx_trace", trace), // link tx processing to action processing
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", txid),
				logger.String("action", action.Code()),
				logger.Int("output_index", outputIndex),
			}, "Transaction request already saved for recovery")
		}

		return nil
	}

	if IsRequest(action) {
		if !a.IsActive() {
			// link tx processing to action processing
			ctx = logger.ContextWithLogFields(ctx, logger.Stringer("tx_trace", trace))

			etx, err := a.createRejection(ctx, transaction, outputIndex, -1,
				platform.NewRejectError(actions.RejectionsInactive, ""))
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

		if feeRate < minFeeRate {
			// link tx processing to action processing
			ctx = logger.ContextWithLogFields(ctx, logger.Stringer("tx_trace", trace))

			etx, err := a.createRejection(ctx, transaction, outputIndex, -1,
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

			return nil
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("tx_trace", trace), // link tx processing to action processing
		logger.Stringer("contract_locking_script", agentLockingScript),
		logger.Stringer("txid", txid),
		logger.Int("output_index", outputIndex),
		logger.String("action_code", action.Code()),
		logger.String("action_name", action.TypeName()),
	}, "Processing action")

	isRequest := IsRequest(action)

	if err := action.Validate(); err != nil {
		if isRequest {
			etx, err := a.createRejection(ctx, transaction, outputIndex, -1,
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
	}, "Processed action")

	if responseEtx != nil {
		if err := a.BroadcastTx(ctx, responseEtx, nil); err != nil {
			return errors.Wrap(err, "broadcast")
		}
	}

	if processError != nil {
		if rejectError, ok := errors.Cause(processError).(platform.RejectError); ok {
			switch act := action.(type) {
			case *actions.Message:
				etx, err := a.createMessageRejection(ctx, transaction, act, outputIndex,
					rejectError)
				if err != nil {
					return errors.Wrap(err, "create rejection")
				}

				if etx != nil {
					if err := a.BroadcastTx(ctx, etx, nil); err != nil {
						return errors.Wrap(err, "broadcast")
					}
				}

			default:
				etx, err := a.createRejection(ctx, transaction, outputIndex, -1, rejectError)
				if err != nil {
					return errors.Wrap(err, "create rejection")
				}

				if etx != nil {
					if err := a.BroadcastTx(ctx, etx, nil); err != nil {
						return errors.Wrap(err, "broadcast")
					}
				}
			}

			return nil
		}

		return errors.Wrap(processError, "process")
	}

	return nil
}

func relevantRequestOutputs(ctx context.Context, etx *expanded_tx.ExpandedTx,
	agentLockingScript bitcoin.Script, isTest bool) ([]int, error) {

	var result []int
	outputCount := etx.OutputCount()
	for index := 0; index < outputCount; index++ {
		output := etx.Output(index)

		action, err := protocol.Deserialize(output.LockingScript, isTest)
		if err != nil {
			continue
		}

		if !IsRequest(action) {
			continue
		}

		agentLockingScripts, err := ReleventAgentLockingScripts(etx, action)
		if err != nil {
			return nil, errors.Wrap(err, "relevant")
		}

		isRelevant := false
		for _, ls := range agentLockingScripts {
			if ls.Equal(agentLockingScript) {
				isRelevant = true
			}
		}

		if isRelevant {
			result = append(result, index)
		}
	}

	return result, nil
}
