package agents

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/platform"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

var (
	ErrNotRelevant = errors.New("Not Relevant")
)

type Config struct {
	IsTest                  bool            `default:"true" envconfig:"IS_TEST" json:"is_test"`
	FeeRate                 float32         `default:"0.05" envconfig:"FEE_RATE" json:"fee_rate"`
	DustFeeRate             float32         `default:"0.00" envconfig:"DUST_FEE_RATE" json:"dust_fee_rate"`
	MinFeeRate              float32         `default:"0.05" envconfig:"MIN_FEE_RATE" json:"min_fee_rate"`
	MultiContractExpiration config.Duration `default:"1h" envconfig:"MULTI_CONTRACT_EXPIRATION" json:"multi_contract_expiration"`
}

func DefaultConfig() Config {
	return Config{
		IsTest:                  true,
		FeeRate:                 0.05,
		DustFeeRate:             0.00,
		MinFeeRate:              0.05,
		MultiContractExpiration: config.NewDuration(time.Hour),
	}
}

type Agent struct {
	key    bitcoin.Key
	config Config

	contract *state.Contract

	lockingScript    bitcoin.Script
	feeLockingScript bitcoin.Script

	store  storage.CopyList
	caches *state.Caches

	broadcaster Broadcaster
	fetcher     Fetcher
	headers     BlockHeaders

	// scheduler and factory are used to schedule tasks that spawn a new agent and perform a
	// function. For example, vote finalization and multi-contract transfer expiration.
	scheduler *platform.Scheduler
	factory   AgentFactory

	lock sync.Mutex
}

type Fetcher interface {
	GetTx(context.Context, bitcoin.Hash32) (*wire.MsgTx, error)
}

type Broadcaster interface {
	BroadcastTx(context.Context, *wire.MsgTx, []uint32) error
}

type BlockHeaders interface {
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)
	GetHeader(context.Context, int) (*wire.BlockHeader, error)
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

type AgentFactory interface {
	GetAgent(ctx context.Context, lockingScript bitcoin.Script) (*Agent, error)
	ReleaseAgent(ctx context.Context, agent *Agent)
}

func NewAgent(key bitcoin.Key, config Config, contract *state.Contract,
	feeLockingScript bitcoin.Script, caches *state.Caches, store storage.CopyList,
	broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders,
	scheduler *platform.Scheduler, factory AgentFactory) (*Agent, error) {

	contract.Lock()
	lockingScript := contract.LockingScript
	contract.Unlock()

	result := &Agent{
		key:              key,
		config:           config,
		contract:         contract,
		lockingScript:    lockingScript,
		feeLockingScript: feeLockingScript,
		caches:           caches,
		store:            store,
		broadcaster:      broadcaster,
		fetcher:          fetcher,
		headers:          headers,
		scheduler:        scheduler,
		factory:          factory,
	}

	return result, nil
}

func (a *Agent) Release(ctx context.Context) {
	a.caches.Contracts.Release(ctx, a.LockingScript())
}

func (a *Agent) Contract() *state.Contract {
	return a.contract
}

func (a *Agent) LockingScript() bitcoin.Script {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.lockingScript
}

func (a *Agent) ContractFee() uint64 {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.Formation.ContractFee
}

func (a *Agent) AdminLockingScript() bitcoin.Script {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.AdminLockingScript()
}

func (a *Agent) ContractExists() bool {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.Formation != nil
}

func (a *Agent) ContractIsExpired(now uint64) bool {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.IsExpired(now)
}

func (a *Agent) MovedTxID() *bitcoin.Hash32 {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.MovedTxID
}

type IdentityOracle struct {
	Index     int
	PublicKey bitcoin.PublicKey
}

func (a *Agent) GetIdentityOracles(ctx context.Context) ([]*IdentityOracle, error) {
	a.lock.Lock()
	contract := a.contract
	a.lock.Unlock()

	contract.Lock()
	defer contract.Unlock()

	if contract.Formation == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contract.LockingScript),
		}, "Missing contract formation")
		return nil, errors.New("Missing contract formation")
	}

	var result []*IdentityOracle
	for i, oracle := range contract.Formation.Oracles {
		if len(oracle.OracleTypes) != 0 {
			isIdentity := false
			for _, typ := range oracle.OracleTypes {
				if typ == actions.ServiceTypeIdentityOracle {
					isIdentity = true
					break
				}
			}

			if !isIdentity {
				continue
			}
		}

		ra, err := bitcoin.DecodeRawAddress(oracle.EntityContract)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", contract.LockingScript),
			}, "Invalid oracle entity contract address %d : %s", i, err)
			continue
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", contract.LockingScript),
			}, "Failed to create oracle entity contract locking script %d : %s", i, err)
			continue
		}

		services, err := a.caches.Services.Get(ctx, lockingScript)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", contract.LockingScript),
				logger.Stringer("service_locking_script", lockingScript),
			}, "Failed to get oracle entity contract service %d : %s", i, err)
			continue
		}

		if services == nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", contract.LockingScript),
				logger.Stringer("service_locking_script", lockingScript),
			}, "Oracle entity contract service not found %d : %s", i, err)
			continue
		}

		for _, service := range services.Services {
			if service.Type != actions.ServiceTypeIdentityOracle {
				continue
			}

			result = append(result, &IdentityOracle{
				Index:     i,
				PublicKey: service.PublicKey,
			})
			break
		}

		a.caches.Services.Release(ctx, lockingScript)
	}

	return result, nil
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

func (a *Agent) BroadcastTx(ctx context.Context, tx *wire.MsgTx, indexes []uint32) error {
	a.lock.Lock()
	broadcaster := a.broadcaster
	a.lock.Unlock()

	if broadcaster == nil {
		return nil
	}

	return broadcaster.BroadcastTx(ctx, tx, indexes)
}

func (a *Agent) FetchTx(ctx context.Context, txid bitcoin.Hash32) (*wire.MsgTx, error) {
	a.lock.Lock()
	fetcher := a.fetcher
	a.lock.Unlock()

	if fetcher == nil {
		return nil, nil
	}

	return fetcher.GetTx(ctx, txid)
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

func (a *Agent) MultiContractExpiration() time.Duration {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.MultiContractExpiration.Duration
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

	agentLockingScript := a.LockingScript()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("txid", transaction.GetTxID()),
		logger.Stringer("contract_locking_script", agentLockingScript))
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

	if err := action.Validate(); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error(), now)), "reject")
	}

	ctx = logger.ContextWithLogFields(ctx, logger.String("action", action.Code()))

	switch act := action.(type) {
	case *actions.ContractOffer:
		return a.processContractOffer(ctx, transaction, act, now)

	case *actions.ContractAmendment:
		return a.processContractAmendment(ctx, transaction, act, now)

	case *actions.ContractFormation:
		return a.processContractFormation(ctx, transaction, act, now)

	case *actions.ContractAddressChange:
		return a.processContractAddressChange(ctx, transaction, act, now)

	case *actions.BodyOfAgreementOffer:
		return a.processBodyOfAgreementOffer(ctx, transaction, act, now)

	case *actions.BodyOfAgreementAmendment:
		return a.processBodyOfAgreementAmendment(ctx, transaction, act, now)

	case *actions.BodyOfAgreementFormation:
		return a.processBodyOfAgreementFormation(ctx, transaction, act, now)

	case *actions.InstrumentDefinition:
		return a.processInstrumentDefinition(ctx, transaction, act, now)

	case *actions.InstrumentModification:
		return a.processInstrumentModification(ctx, transaction, act, now)

	case *actions.InstrumentCreation:
		return a.processInstrumentCreation(ctx, transaction, act, now)

	case *actions.Transfer:
		return a.processTransfer(ctx, transaction, act, now)

	case *actions.Settlement:
		return a.processSettlement(ctx, transaction, act, now)

	case *actions.Proposal:
		return a.processProposal(ctx, transaction, act, now)

	case *actions.Vote:
		return a.processVote(ctx, transaction, act, now)

	case *actions.BallotCast:
		return a.processBallotCast(ctx, transaction, act, now)

	case *actions.BallotCounted:
		return a.processBallotCounted(ctx, transaction, act, now)

	case *actions.Result:
		return a.processVoteResult(ctx, transaction, act, now)

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
