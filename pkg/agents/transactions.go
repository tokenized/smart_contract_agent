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

type ActionAgent struct {
	LockingScript bitcoin.Script
	IsRequest     bool // this action is a request to this agent.
}

type Action struct {
	Agents      []ActionAgent
	OutputIndex int
	Action      actions.Action
}

func (a Action) IsRelevant(agentLockingScript bitcoin.Script) bool {
	for _, agent := range a.Agents {
		if agent.LockingScript.Equal(agentLockingScript) {
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

		agents, err := GetActionAgents(transaction, action)
		if err != nil {
			return nil, errors.Wrapf(err, "output %d", index)
		}

		if len(agents) == 0 {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("txid", transaction.TxID()),
				logger.String("action_code", action.Code()),
				logger.Int("output_index", index),
			}, "No relevant agent locking scripts")
			continue
		}

		result = append(result, Action{
			Agents:      agents,
			OutputIndex: index,
			Action:      action,
		})
	}

	return result, nil
}

// GetActionAgents returns the agent actions that are relevant to this action.
func GetActionAgents(transaction expanded_tx.TransactionWithOutputs,
	action actions.Action) ([]ActionAgent, error) {

	switch act := action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange,
		*actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment,
		*actions.InstrumentDefinition, *actions.InstrumentModification, *actions.Proposal,
		*actions.BallotCast, *actions.Order:

		// Request action where first output is the contract agent.
		return []ActionAgent{
			{
				LockingScript: transaction.Output(0).LockingScript,
				IsRequest:     true,
			},
		}, nil

	case *actions.ContractFormation, *actions.BodyOfAgreementFormation, *actions.InstrumentCreation,
		*actions.Vote, *actions.BallotCounted, *actions.Result, *actions.Freeze, *actions.Thaw,
		*actions.Confiscation, *actions.DeprecatedReconciliation:

		// Response actions where first input is the contract agent.
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return nil, errors.Wrap(err, "input locking script")
		}

		return []ActionAgent{
			{
				LockingScript: inputOutput.LockingScript,
				IsRequest:     false,
			},
		}, nil

	case *actions.Transfer:
		outputCount := transaction.OutputCount()
		var result []ActionAgent
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= outputCount {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, outputCount)
			}

			lockingScript := transaction.Output(int(instrument.ContractIndex)).LockingScript
			result = append(result, ActionAgent{
				LockingScript: lockingScript,
				IsRequest:     true,
			})
		}

		return result, nil

	case *actions.Settlement:
		inputCount := transaction.InputCount()
		var result []ActionAgent
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= inputCount {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, inputCount)
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = append(result, ActionAgent{
				LockingScript: inputOutput.LockingScript,
				IsRequest:     false,
			})
		}

		return result, nil

	case *actions.RectificationSettlement:
		inputCount := transaction.InputCount()
		var result []ActionAgent
		for _, instrument := range act.Instruments {
			if int(instrument.ContractIndex) >= inputCount {
				return nil, errors.Wrapf(ErrInvalidAction, "index out of range : %d >= %d",
					instrument.ContractIndex, inputCount)
			}

			inputOutput, err := transaction.InputOutput(int(instrument.ContractIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = append(result, ActionAgent{
				LockingScript: inputOutput.LockingScript,
				IsRequest:     false,
			})
		}

		return result, nil

	case *actions.Message:
		inputCount := transaction.InputCount()
		var result []ActionAgent
		for _, senderIndex := range act.SenderIndexes {
			if int(senderIndex) >= inputCount {
				return nil, errors.Wrapf(ErrInvalidAction, "sender index out of range : %d >= %d",
					senderIndex, inputCount)
			}

			inputOutput, err := transaction.InputOutput(int(senderIndex))
			if err != nil {
				return nil, errors.Wrap(err, "input locking script")
			}
			result = append(result, ActionAgent{
				LockingScript: inputOutput.LockingScript,
				IsRequest:     false, // sent from this contract
			})
		}

		outputCount := transaction.OutputCount()
		for _, receiverIndex := range act.ReceiverIndexes {
			if int(receiverIndex) >= outputCount {
				return nil, errors.Wrapf(ErrInvalidAction, "receiver index out of range : %d >= %d",
					receiverIndex, outputCount)
			}

			lockingScript := transaction.Output(int(receiverIndex)).LockingScript
			result = append(result, ActionAgent{
				LockingScript: lockingScript,
				IsRequest:     true, // sent to this contract
			})
		}

		return result, nil

	case *actions.Rejection:
		var result []ActionAgent

		// First input
		inputOutput, err := transaction.InputOutput(0)
		if err != nil {
			return nil, errors.Wrap(err, "input locking script")
		}
		result = append(result, ActionAgent{
			LockingScript: inputOutput.LockingScript,
			IsRequest:     false,
		})

		outputCount := transaction.OutputCount()
		if int(act.RejectAddressIndex) >= outputCount {
			return nil, errors.Wrapf(ErrInvalidAction, "reject index out of range : %d >= %d",
				int(act.RejectAddressIndex), outputCount)
		}

		lockingScript := transaction.Output(int(act.RejectAddressIndex)).LockingScript
		result = append(result, ActionAgent{
			LockingScript: lockingScript,
			IsRequest:     true,
		})

		if len(act.AddressIndexes) == 0 {
			// First output
			lockingScript := transaction.Output(0).LockingScript
			result = append(result, ActionAgent{
				LockingScript: lockingScript,
				IsRequest:     true,
			})
		} else {
			for _, addressIndex := range act.AddressIndexes {
				if int(addressIndex) >= outputCount {
					return nil, errors.Wrapf(ErrInvalidAction, "address index out of range : %d >= %d",
						addressIndex, outputCount)
				}

				lockingScript := transaction.Output(int(addressIndex)).LockingScript
				result = append(result, ActionAgent{
					LockingScript: lockingScript,
					IsRequest:     true,
				})
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
		for _, actionAgent := range action.Agents {
			if actionAgent.LockingScript.Equal(agentLockingScript) {
				relevantActions = append(relevantActions, action)

				if actionAgent.IsRequest {
					containsRequest = true
				}

				break
			}
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
		for _, actionAgent := range action.Agents {
			if !actionAgent.LockingScript.Equal(agentLockingScript) {
				continue
			}

			if err := a.processAction(ctx, agentLockingScript, transaction, txid, action.Action,
				action.OutputIndex, actionAgent.IsRequest, config.RecoveryMode, feeRate, minFeeRate,
				trace); err != nil {
				return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
			}
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
		for _, actionAgent := range action.Agents {
			if !actionAgent.LockingScript.Equal(agentLockingScript) {
				continue
			}

			if actionAgent.IsRequest {
				processed := transaction.GetContractProcessed(a.ContractHash(), action.OutputIndex)
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
			} else {
				// If it isn't a request then we can process it like normal.
				if err := a.processAction(ctx, agentLockingScript, transaction, txid, action.Action,
					action.OutputIndex, actionAgent.IsRequest, false, 1.0, 0.0, trace); err != nil {
					return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
				}
			}
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, agentLockingScript bitcoin.Script,
	transaction *transactions.Transaction, txid bitcoin.Hash32, action actions.Action,
	actionIndex int, isRequest, inRecovery bool, feeRate, minFeeRate float64,
	trace uuid.UUID) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("action_trace", uuid.New()))
	start := time.Now()

	processed := transaction.GetContractProcessed(a.ContractHash(), actionIndex)
	if len(processed) > 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("tx_trace", trace), // link tx processing to action processing
			logger.Stringer("contract_locking_script", agentLockingScript),
			logger.Stringer("txid", txid),
			logger.Int("action_index", actionIndex),
			logger.String("action_code", action.Code()),
			logger.String("action_name", action.TypeName()),
			logger.Stringer("response_txid", processed[0].ResponseTxID),
		}, "Action already processed")

		return nil
	}

	if inRecovery && isRequest {
		if added, err := a.addRecoveryRequest(ctx, agentLockingScript, txid,
			actionIndex); err != nil {
			return errors.Wrap(err, "recovery request")
		} else if added {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("tx_trace", trace), // link tx processing to action processing
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", txid),
				logger.String("action", action.Code()),
				logger.Int("action_index", actionIndex),
			}, "Saved transaction request for recovery")
		} else {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("tx_trace", trace), // link tx processing to action processing
				logger.Stringer("contract_locking_script", agentLockingScript),
				logger.Stringer("txid", txid),
				logger.String("action", action.Code()),
				logger.Int("action_index", actionIndex),
			}, "Transaction request already saved for recovery")
		}

		return nil
	}

	if isRequest {
		if !a.IsActive() {
			// link tx processing to action processing
			ctx = logger.ContextWithLogFields(ctx, logger.Stringer("tx_trace", trace))

			etx, err := a.createRejection(ctx, transaction, actionIndex, -1,
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

			etx, err := a.createRejection(ctx, transaction, actionIndex, -1,
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
		logger.Int("action_index", actionIndex),
		logger.String("action_code", action.Code()),
		logger.String("action_name", action.TypeName()),
		logger.Bool("is_request", isRequest),
	}, "Processing action")

	if err := action.Validate(); err != nil {
		if isRequest {
			etx, err := a.createRejection(ctx, transaction, actionIndex, -1,
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
				logger.Int("action_index", actionIndex),
				logger.String("action_code", action.Code()),
				logger.String("action_name", action.TypeName()),
			}, "Response action invalid : %s", err)
		}

		return nil
	}

	var responseEtx *expanded_tx.ExpandedTx
	var processError error
	switch act := action.(type) {
	case *actions.ContractOffer:
		responseEtx, processError = a.processContractOffer(ctx, transaction, act, actionIndex)

	case *actions.ContractAmendment:
		responseEtx, processError = a.processContractAmendment(ctx, transaction, act, actionIndex)

	case *actions.ContractFormation:
		processError = a.processContractFormation(ctx, transaction, act, actionIndex)

	case *actions.ContractAddressChange:
		responseEtx, processError = a.processContractAddressChange(ctx, transaction, act,
			actionIndex)

	case *actions.BodyOfAgreementOffer:
		responseEtx, processError = a.processBodyOfAgreementOffer(ctx, transaction, act,
			actionIndex)

	case *actions.BodyOfAgreementAmendment:
		responseEtx, processError = a.processBodyOfAgreementAmendment(ctx, transaction, act,
			actionIndex)

	case *actions.BodyOfAgreementFormation:
		processError = a.processBodyOfAgreementFormation(ctx, transaction, act, actionIndex)

	case *actions.InstrumentDefinition:
		responseEtx, processError = a.processInstrumentDefinition(ctx, transaction, act,
			actionIndex)

	case *actions.InstrumentModification:
		responseEtx, processError = a.processInstrumentModification(ctx, transaction, act,
			actionIndex)

	case *actions.InstrumentCreation:
		processError = a.processInstrumentCreation(ctx, transaction, act, actionIndex)

	case *actions.Transfer:
		responseEtx, processError = a.processTransfer(ctx, transaction, act, actionIndex)

	case *actions.Settlement:
		processError = a.processSettlement(ctx, transaction, act, actionIndex)

	case *actions.RectificationSettlement:
		// TODO Create function that watches for "double spent" requests and sends Rectification
		// Settlements. --ce
		processError = a.processRectificationSettlement(ctx, transaction, act, actionIndex)

	case *actions.Proposal:
		responseEtx, processError = a.processProposal(ctx, transaction, act, actionIndex)

	case *actions.Vote:
		processError = a.processVote(ctx, transaction, act, actionIndex)

	case *actions.BallotCast:
		responseEtx, processError = a.processBallotCast(ctx, transaction, act, actionIndex)

	case *actions.BallotCounted:
		processError = a.processBallotCounted(ctx, transaction, act, actionIndex)

	case *actions.Result:
		processError = a.processVoteResult(ctx, transaction, act, actionIndex)

	case *actions.Order:
		responseEtx, processError = a.processOrder(ctx, transaction, act, actionIndex)

	case *actions.Freeze:
		processError = a.processFreeze(ctx, transaction, act, actionIndex)

	case *actions.Thaw:
		processError = a.processThaw(ctx, transaction, act, actionIndex)

	case *actions.Confiscation:
		processError = a.processConfiscation(ctx, transaction, act, actionIndex)

	case *actions.DeprecatedReconciliation:
		processError = a.processReconciliation(ctx, transaction, act, actionIndex)

	case *actions.Message:
		responseEtx, processError = a.processMessage(ctx, transaction, act, actionIndex)

	case *actions.Rejection:
		responseEtx, processError = a.processRejection(ctx, transaction, act, actionIndex)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		logger.String("action_code", action.Code()),
	}, "Processed action")

	if responseEtx != nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("response_txid", responseEtx.TxID()),
		}, "Broadcasting response")
		if err := a.BroadcastTx(ctx, responseEtx, nil); err != nil {
			return errors.Wrap(err, "broadcast")
		}
	}

	if processError != nil {
		if rejectError, ok := errors.Cause(processError).(platform.RejectError); ok {
			if isRequest {
				logger.Warn(ctx, "Received reject from response processing : %s", processError)

				switch act := action.(type) {
				case *actions.Message:
					etxs, err := a.createMessageRejection(ctx, transaction, act, actionIndex,
						rejectError)
					if err != nil {
						return errors.Wrap(err, "create rejection")
					}

					for _, etx := range etxs {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("response_txid", etx.TxID()),
						}, "Broadcasting response")
						if err := a.BroadcastTx(ctx, etx, nil); err != nil {
							return errors.Wrap(err, "broadcast")
						}
					}

				default:
					etx, err := a.createRejection(ctx, transaction, actionIndex, -1, rejectError)
					if err != nil {
						return errors.Wrap(err, "create rejection")
					}

					if etx != nil {
						logger.InfoWithFields(ctx, []logger.Field{
							logger.Stringer("response_txid", etx.TxID()),
						}, "Broadcasting response")
						if err := a.BroadcastTx(ctx, etx, nil); err != nil {
							return errors.Wrap(err, "broadcast")
						}
					}
				}
			} else {
				logger.Error(ctx, "Received reject from response processing : %s", processError)
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

		agentActions, err := GetActionAgents(etx, action)
		if err != nil {
			return nil, errors.Wrap(err, "relevant")
		}

		isRelevant := false
		for _, agentAction := range agentActions {
			if agentAction.IsRequest && agentAction.LockingScript.Equal(agentLockingScript) {
				isRelevant = true
			}
		}

		if isRelevant {
			result = append(result, index)
		}
	}

	return result, nil
}
