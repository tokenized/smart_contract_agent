package agents

import (
	"context"
	"fmt"
	"sync"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
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

	contract *state.Contract

	feeLockingScript bitcoin.Script

	contracts     *state.ContractCache
	balances      *state.BalanceCache
	transactions  *state.TransactionCache
	subscriptions *state.SubscriptionCache

	broadcaster Broadcaster

	feeRate, dustFeeRate float32
	isTest               bool

	lock sync.Mutex
}

type Broadcaster interface {
	BroadcastTx(context.Context, *wire.MsgTx) error
}

type TransactionWithOutputs interface {
	TxID() bitcoin.Hash32
	GetMsgTx() *wire.MsgTx

	InputCount() int
	Input(index int) *wire.TxIn
	InputOutput(index int) (*wire.TxOut, error) // The output being spent by the input

	OutputCount() int
	Output(index int) *wire.TxOut
}

func NewAgent(key bitcoin.Key, contract *state.Contract, feeLockingScript bitcoin.Script,
	contracts *state.ContractCache, balances *state.BalanceCache,
	transactions *state.TransactionCache, subscriptions *state.SubscriptionCache,
	broadcaster Broadcaster, feeRate, dustFeeRate float32, isTest bool) (*Agent, error) {

	result := &Agent{
		key:              key,
		contract:         contract,
		feeLockingScript: feeLockingScript,
		contracts:        contracts,
		balances:         balances,
		transactions:     transactions,
		subscriptions:    subscriptions,
		broadcaster:      broadcaster,
		feeRate:          feeRate,
		dustFeeRate:      dustFeeRate,
		isTest:           isTest,
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

func (a *Agent) FeeLockingScript() bitcoin.Script {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.feeLockingScript
}

func (a *Agent) Key() bitcoin.Key {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.key
}

func (a *Agent) BroadcastTx(ctx context.Context, tx *wire.MsgTx) error {
	a.lock.Lock()
	broadcaster := a.broadcaster
	a.lock.Unlock()

	return broadcaster.BroadcastTx(ctx, tx)
}

func (a *Agent) IsTest() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.isTest
}

func (a *Agent) FeeRate() float32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.feeRate
}

func (a *Agent) DustFeeRate() float32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.dustFeeRate
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

func (a *Agent) Process(ctx context.Context, transaction *state.Transaction,
	actions []actions.Action) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", transaction.GetTxID()))
	logger.Info(ctx, "Processing transaction")

	for i, action := range actions {
		if err := a.processAction(ctx, transaction, action); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, transaction *state.Transaction,
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
