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

type Config struct {
	IsTest      bool    `default:"true" envconfig:"IS_TEST" json:"is_test"`
	FeeRate     float32 `default:"0.05" envconfig:"FEE_RATE" json:"fee_rate"`
	DustFeeRate float32 `default:"0.00" envconfig:"DUST_FEE_RATE" json:"dust_fee_rate"`
	MinFeeRate  float32 `default:"0.05" envconfig:"MIN_FEE_RATE" json:"min_fee_rate"`
}

type Agent struct {
	key    bitcoin.Key
	config Config

	contract *state.Contract

	feeLockingScript bitcoin.Script

	contracts     *state.ContractCache
	balances      *state.BalanceCache
	transactions  *state.TransactionCache
	subscriptions *state.SubscriptionCache

	broadcaster Broadcaster

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

func NewAgent(key bitcoin.Key, config Config, contract *state.Contract,
	feeLockingScript bitcoin.Script, contracts *state.ContractCache, balances *state.BalanceCache,
	transactions *state.TransactionCache, subscriptions *state.SubscriptionCache,
	broadcaster Broadcaster) (*Agent, error) {

	result := &Agent{
		key:              key,
		config:           config,
		contract:         contract,
		feeLockingScript: feeLockingScript,
		contracts:        contracts,
		balances:         balances,
		transactions:     transactions,
		subscriptions:    subscriptions,
		broadcaster:      broadcaster,
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

func (a *Agent) ContractFee() uint64 {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.Formation.ContractFee
}

func (a *Agent) AdminLockingScript() bitcoin.Script {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.AdminLockingScript()
}

func (a *Agent) ContractIsExpired(now uint64) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.IsExpired(now)
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

	return a.config.IsTest
}

func (a *Agent) FeeRate() float32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.FeeRate
}

func (a *Agent) DustFeeRate() float32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.DustFeeRate
}

func (a *Agent) MinFeeRate() float32 {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.MinFeeRate
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
	actions []actions.Action, now uint64) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", transaction.GetTxID()))
	logger.Info(ctx, "Processing transaction")

	// TODO If the contract locking script has changed, then reject all actions. --ce

	// TODO If the contract is expired, then reject all actions. --ce

	for i, action := range actions {
		if err := a.processAction(ctx, transaction, action, now); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, transaction *state.Transaction,
	action actions.Action, now uint64) error {

	switch act := action.(type) {
	case *actions.ContractOffer:
	case *actions.ContractAmendment:

	case *actions.ContractFormation:
		return a.processContractFormation(ctx, transaction, act, now)

	case *actions.ContractAddressChange:

	case *actions.BodyOfAgreementOffer:
	case *actions.BodyOfAgreementAmendment:

	case *actions.BodyOfAgreementFormation:
		return a.processBodyOfAgreementFormation(ctx, transaction, act, now)

	case *actions.InstrumentDefinition:
	case *actions.InstrumentModification:

	case *actions.InstrumentCreation:
		return a.processInstrumentCreation(ctx, transaction, act, now)

	case *actions.Transfer:
		return a.processTransfer(ctx, transaction, act, now)

	case *actions.Settlement:
		return a.processSettlement(ctx, transaction, act, now)

	case *actions.Proposal:

	case *actions.Vote:
		return a.processVote(ctx, transaction, act, now)

	case *actions.BallotCast:

	case *actions.BallotCounted:
		return a.processBallotCounted(ctx, transaction, act, now)

	case *actions.Result:
		return a.processGovernanceResult(ctx, transaction, act, now)

	case *actions.Order:

	case *actions.Freeze:
		return a.processFreeze(ctx, transaction, act, now)

	case *actions.Thaw:
		return a.processThaw(ctx, transaction, act, now)

	case *actions.Confiscation:
		return a.processConfiscation(ctx, transaction, act, now)

	case *actions.Reconciliation:
		return a.processReconciliation(ctx, transaction, act, now)

	case *actions.Message:
		return a.processMessage(ctx, transaction, act, now)

	case *actions.Rejection:
		return a.processRejection(ctx, transaction, act, now)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
