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

	contract      *state.Contract
	contracts     *state.ContractCache
	balances      *state.BalanceCache
	transactions  *state.TransactionCache
	subscriptions *state.SubscriptionCache

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
	balances *state.BalanceCache, transactions *state.TransactionCache,
	subscriptions *state.SubscriptionCache) (*Agent, error) {

	result := &Agent{
		key:           key,
		contract:      contract,
		contracts:     contracts,
		balances:      balances,
		transactions:  transactions,
		subscriptions: subscriptions,
	}

	return result, nil
}

func (a *Agent) Release(ctx context.Context) {
	a.contracts.Release(ctx, a.LockingScript())
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

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", transaction.TxID()))
	logger.Info(ctx, "Processing transaction")

	for i, action := range actions {
		if err := a.processAction(ctx, transaction, action); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, transaction TransactionWithOutputs,
	action actions.Action) error {

	switch act := action.(type) {
	case *actions.ContractOffer:
	case *actions.ContractAmendment:

	case *actions.ContractFormation:
		return a.processContractFormation(ctx, transaction, act)

	case *actions.ContractAddressChange:

	case *actions.BodyOfAgreementOffer:
	case *actions.BodyOfAgreementAmendment:

	case *actions.BodyOfAgreementFormation:
		return a.processBodyOfAgreementFormation(ctx, transaction, act)

	case *actions.InstrumentDefinition:
	case *actions.InstrumentModification:

	case *actions.InstrumentCreation:
		return a.processInstrumentCreation(ctx, transaction, act)

	case *actions.Transfer:
		return a.processTransfer(ctx, transaction, act)

	case *actions.Settlement:
		return a.processSettlement(ctx, transaction, act)

	case *actions.Proposal:

	case *actions.Vote:
		return a.processVote(ctx, transaction, act)

	case *actions.BallotCast:

	case *actions.BallotCounted:
		return a.processBallotCounted(ctx, transaction, act)

	case *actions.Result:
		return a.processGovernanceResult(ctx, transaction, act)

	case *actions.Order:

	case *actions.Freeze:
		return a.processFreeze(ctx, transaction, act)

	case *actions.Thaw:
		return a.processThaw(ctx, transaction, act)

	case *actions.Confiscation:
		return a.processConfiscation(ctx, transaction, act)

	case *actions.Reconciliation:
		return a.processReconciliation(ctx, transaction, act)

	case *actions.Message:
		return a.processMessage(ctx, transaction, act)

	case *actions.Rejection:
		return a.processRejection(ctx, transaction, act)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
