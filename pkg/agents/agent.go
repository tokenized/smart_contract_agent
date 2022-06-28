package agents

import (
	"context"
	"fmt"
	"sync"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

var (
	ErrNotRelevant = errors.New("Not Relevant")
)

type Agent struct {
	key bitcoin.Key

	contract     *state.Contract
	contracts    *state.ContractCache
	balances     *state.BalanceCache
	transactions *state.TransactionCache

	isTest bool

	lock sync.Mutex
}

type TransactionWithOutputs interface {
	TxID() bitcoin.Hash32

	InputCount() int
	Input(index int) *wire.TxIn
	InputLockingScript(index int) (bitcoin.Script, error)

	OutputCount() int
	Output(index int) *wire.TxOut
}

func NewAgent(key bitcoin.Key, contract *state.Contract, contracts *state.ContractCache,
	balances *state.BalanceCache, transactions *state.TransactionCache) (*Agent, error) {

	result := &Agent{
		key:          key,
		contract:     contract,
		contracts:    contracts,
		balances:     balances,
		transactions: transactions,
	}

	return result, nil
}

func (a *Agent) Release(ctx context.Context, contracts *state.ContractCache) {
	contracts.Release(ctx, a.LockingScript())
}

func (a *Agent) LockingScript() bitcoin.Script {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.LockingScript
}

func (a *Agent) IsTest() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.isTest
}

func (a *Agent) ActionIsSupported(action actions.Action) bool {
	switch action.(type) {
	case *actions.ContractOffer, *actions.ContractFormation, *actions.ContractAmendment,
		*actions.ContractAddressChange:
		return true

	case *actions.BodyOfAgreementOffer, *actions.BodyOfAgreementFormation,
		*actions.BodyOfAgreementAmendment:
		return true

	case *actions.InstrumentDefinition, *actions.InstrumentCreation,
		*actions.InstrumentModification:
		return true

	case *actions.Transfer, *actions.Settlement:
		return true

	case *actions.Proposal, *actions.Vote, *actions.BallotCast, *actions.BallotCounted,
		*actions.Result:
		return true

	case *actions.Order, *actions.Freeze, *actions.Thaw, *actions.Confiscation,
		*actions.Reconciliation:
		return true

	case *actions.Message, *actions.Rejection:
		return true

	default:
		return false
	}
}

func (a *Agent) Process(ctx context.Context, transaction TransactionWithOutputs,
	actions []actions.Action) error {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", transaction.TxID()),
	}, "Processing transaction")

	lockingScript := a.LockingScript()

	inputCount := transaction.InputCount()
	for index := 0; index < inputCount; index++ {
		inputLockingScript, err := transaction.InputLockingScript(index)
		if err != nil {
			return errors.Wrapf(err, "input locking script %d", index)
		}

		if inputLockingScript.Equal(lockingScript) {
			for _, action := range actions {
				if err := a.processResponseAction(ctx, transaction, index, action); err != nil {
					return errors.Wrap(err, "process response")
				}
			}
		}
	}

	outputCount := transaction.OutputCount()
	for index := 0; index < outputCount; index++ {
		output := transaction.Output(index)
		if output.LockingScript.Equal(lockingScript) {
			for _, action := range actions {
				if err := a.processRequestAction(ctx, transaction, index, action); err != nil {
					return errors.Wrap(err, "process response")
				}
			}
		}
	}

	return nil
}

func (a *Agent) processResponseAction(ctx context.Context, transaction TransactionWithOutputs,
	index int, action actions.Action) error {

	switch act := action.(type) {
	case *actions.ContractFormation:
		return a.processContractFormation(ctx, transaction, index, act)

	case *actions.BodyOfAgreementFormation:
		return a.processBodyOfAgreementFormation(ctx, transaction, index, act)

	case *actions.InstrumentCreation:
		return a.processInstrumentCreation(ctx, transaction, index, act)

	case *actions.Settlement:
		return a.processSettlement(ctx, transaction, index, act)

	case *actions.Vote:
		return a.processVote(ctx, transaction, index, act)

	case *actions.BallotCounted:
		return a.processBallotCounted(ctx, transaction, index, act)

	case *actions.Result:
		return a.processGovernanceResult(ctx, transaction, index, act)

	case *actions.Freeze:
		return a.processFreeze(ctx, transaction, index, act)

	case *actions.Thaw:
		return a.processThaw(ctx, transaction, index, act)

	case *actions.Confiscation:
		return a.processConfiscation(ctx, transaction, index, act)

	case *actions.Reconciliation:
		return a.processReconciliation(ctx, transaction, index, act)

	case *actions.Rejection:
		return a.processOutgoingRejection(ctx, transaction, index, act)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}

func (a *Agent) processRequestAction(ctx context.Context, transaction TransactionWithOutputs,
	index int, action actions.Action) error {

	switch act := action.(type) {
	case *actions.ContractOffer:
	case *actions.ContractAmendment:
	case *actions.ContractAddressChange:
	case *actions.BodyOfAgreementOffer:
	case *actions.BodyOfAgreementAmendment:
	case *actions.InstrumentDefinition:
	case *actions.InstrumentModification:
	case *actions.Transfer:
		return a.processTransfer(ctx, transaction, index, act)

	case *actions.Proposal:
	case *actions.BallotCast:
	case *actions.Order:
	case *actions.Message:
		return a.processIncomingMessage(ctx, transaction, index, act)

	case *actions.Rejection:
		return a.processIncomingRejection(ctx, transaction, index, act)

	default:
		return fmt.Errorf("Action %s not supported", action.Code())
	}

	return nil
}
