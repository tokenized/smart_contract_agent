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

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ErrNotRelevant = errors.New("Not Relevant")
)

type Config struct {
	IsTest                  bool            `default:"true" envconfig:"IS_TEST" json:"is_test"`
	FeeRate                 float32         `default:"0.05" envconfig:"FEE_RATE" json:"fee_rate"`
	DustFeeRate             float32         `default:"0.0" envconfig:"DUST_FEE_RATE" json:"dust_fee_rate"`
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

type Broadcaster interface {
	BroadcastTx(context.Context, *wire.MsgTx, []uint32) error
}

type Fetcher interface {
	GetTx(context.Context, bitcoin.Hash32) (*wire.MsgTx, error)
}

type BlockHeaders interface {
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)
	GetHeader(context.Context, int) (*wire.BlockHeader, error)
}

type AgentFactory interface {
	GetAgent(ctx context.Context, lockingScript bitcoin.Script) (*Agent, error)
}

func NewAgent(ctx context.Context, key bitcoin.Key, lockingScript bitcoin.Script, config Config,
	feeLockingScript bitcoin.Script, caches *state.Caches, store storage.CopyList,
	broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders, scheduler *platform.Scheduler,
	factory AgentFactory) (*Agent, error) {

	contract, err := caches.Contracts.Get(ctx, lockingScript)
	if err != nil {
		return nil, errors.Wrap(err, "get contract")
	}

	if contract == nil {
		return nil, errors.New("Contract not found")
	}

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

func (a *Agent) Copy(ctx context.Context) *Agent {
	// Call get on the contract again to increment its users so the release of this copy will be
	// accurate.
	a.caches.Contracts.Get(ctx, a.LockingScript())

	return a
}

func (a *Agent) Release(ctx context.Context) {
	a.caches.Contracts.Release(ctx, a.LockingScript())
}

func (a *Agent) Contract() *state.Contract {
	return a.contract
}

func (a *Agent) ContractHash() state.ContractHash {
	a.lock.Lock()
	defer a.lock.Unlock()

	return state.CalculateContractHash(a.lockingScript)
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

func (a *Agent) CheckContractIsAvailable(now uint64) error {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.CheckIsAvailable(now)
}

type IdentityOracle struct {
	Index     int
	PublicKey bitcoin.PublicKey
}

func (a *Agent) GetIdentityOracles(ctx context.Context) ([]*IdentityOracle, error) {
	a.contract.Lock()
	defer a.contract.Unlock()

	if a.contract.Formation == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", a.contract.LockingScript),
		}, "Missing contract formation")
		return nil, errors.New("Missing contract formation")
	}

	var result []*IdentityOracle
	for i, oracle := range a.contract.Formation.Oracles {
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
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
			}, "Invalid oracle entity contract address %d : %s", i, err)
			continue
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
			}, "Failed to create oracle entity contract locking script %d : %s", i, err)
			continue
		}

		services, err := a.caches.Services.Get(ctx, lockingScript)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
				logger.Stringer("service_locking_script", lockingScript),
			}, "Failed to get oracle entity contract service %d : %s", i, err)
			continue
		}

		if services == nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
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

	case *actions.Transfer, *actions.Settlement, *actions.RectificationSettlement:
		return true

	case *actions.Proposal, *actions.Vote, *actions.BallotCast, *actions.BallotCounted,
		*actions.Result:
		return true

	case *actions.Order, *actions.Freeze, *actions.Thaw, *actions.Confiscation,
		*actions.DeprecatedReconciliation:
		return true

	case *actions.Message, *actions.Rejection:
		return true

	default:
		return false
	}
}

func (a *Agent) Process(ctx context.Context, transaction *state.Transaction,
	actions []Action, now uint64) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing transaction")

	for i, action := range actions {
		if err := a.processAction(ctx, transaction, txid, action.Action, action.OutputIndex,
			now); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}

func (a *Agent) processAction(ctx context.Context, transaction *state.Transaction,
	txid bitcoin.Hash32, action actions.Action, outputIndex int, now uint64) error {

	processed := transaction.ContractProcessed(a.ContractHash(), outputIndex)
	if len(processed) > 0 {
		if processed[0].ResponseTxID == nil {
			println("response txid is nil")
		}
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

	if err := action.Validate(); err != nil {
		return errors.Wrap(a.sendRejection(ctx, transaction, outputIndex,
			platform.NewRejectError(actions.RejectionsMsgMalformed, err.Error()), now), "reject")
	}

	switch act := action.(type) {
	case *actions.ContractOffer:
		return a.processContractOffer(ctx, transaction, act, outputIndex, now)

	case *actions.ContractAmendment:
		return a.processContractAmendment(ctx, transaction, act, outputIndex, now)

	case *actions.ContractFormation:
		return a.processContractFormation(ctx, transaction, act, outputIndex, now)

	case *actions.ContractAddressChange:
		return a.processContractAddressChange(ctx, transaction, act, outputIndex, now)

	case *actions.BodyOfAgreementOffer:
		return a.processBodyOfAgreementOffer(ctx, transaction, act, outputIndex, now)

	case *actions.BodyOfAgreementAmendment:
		return a.processBodyOfAgreementAmendment(ctx, transaction, act, outputIndex, now)

	case *actions.BodyOfAgreementFormation:
		return a.processBodyOfAgreementFormation(ctx, transaction, act, outputIndex, now)

	case *actions.InstrumentDefinition:
		return a.processInstrumentDefinition(ctx, transaction, act, outputIndex, now)

	case *actions.InstrumentModification:
		return a.processInstrumentModification(ctx, transaction, act, outputIndex, now)

	case *actions.InstrumentCreation:
		return a.processInstrumentCreation(ctx, transaction, act, outputIndex, now)

	case *actions.Transfer:
		return a.processTransfer(ctx, transaction, act, outputIndex, now)

	case *actions.Settlement:
		return a.processSettlement(ctx, transaction, act, outputIndex, now)

	case *actions.RectificationSettlement:
		// TODO Create function that watches for "double spent" requests and sends Rectification
		// Settlements. --ce
		return a.processRectificationSettlement(ctx, transaction, act, outputIndex, now)

	case *actions.Proposal:
		return a.processProposal(ctx, transaction, act, outputIndex, now)

	case *actions.Vote:
		return a.processVote(ctx, transaction, act, outputIndex, now)

	case *actions.BallotCast:
		return a.processBallotCast(ctx, transaction, act, outputIndex, now)

	case *actions.BallotCounted:
		return a.processBallotCounted(ctx, transaction, act, outputIndex, now)

	case *actions.Result:
		return a.processVoteResult(ctx, transaction, act, outputIndex, now)

	case *actions.Order:
		return a.processOrder(ctx, transaction, act, outputIndex, now)

	case *actions.Freeze:
		return a.processFreeze(ctx, transaction, act, outputIndex, now)

	case *actions.Thaw:
		return a.processThaw(ctx, transaction, act, outputIndex, now)

	case *actions.Confiscation:
		return a.processConfiscation(ctx, transaction, act, outputIndex, now)

	case *actions.DeprecatedReconciliation:
		return a.processReconciliation(ctx, transaction, act, outputIndex, now)

	case *actions.Message:
		return a.processMessage(ctx, transaction, act, outputIndex, now)

	case *actions.Rejection:
		return a.processRejection(ctx, transaction, act, outputIndex, now)

	default:
		return fmt.Errorf("Action not supported: %s", action.Code())
	}

	return nil
}
