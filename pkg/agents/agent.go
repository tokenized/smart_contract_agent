package agents

import (
	"context"
	"sync"
	"time"

	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
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
	DustFeeRate             float32         `default:"0.0" envconfig:"DUST_FEE_RATE" json:"dust_fee_rate"`
	MinFeeRate              float32         `default:"0.05" envconfig:"MIN_FEE_RATE" json:"min_fee_rate"`
	MultiContractExpiration config.Duration `default:"1h" envconfig:"MULTI_CONTRACT_EXPIRATION" json:"multi_contract_expiration"`
	RecoveryMode            bool            `default:"false" envconfig:"RECOVERY_MODE" json:"recovery_mode"`
}

func DefaultConfig() Config {
	return Config{
		IsTest:                  true,
		FeeRate:                 0.05,
		DustFeeRate:             0.00,
		MinFeeRate:              0.05,
		MultiContractExpiration: config.NewDuration(time.Hour),
		RecoveryMode:            false,
	}
}

type Agent struct {
	key    bitcoin.Key
	config Config

	contract *state.Contract

	lockingScript    bitcoin.Script
	feeLockingScript bitcoin.Script

	store         storage.CopyList
	caches        *state.Caches
	balanceLocker state.BalanceLocker

	broadcaster Broadcaster
	fetcher     Fetcher
	headers     BlockHeaders

	// scheduler and factory are used to schedule tasks that spawn a new agent and perform a
	// function. For example, vote finalization and multi-contract transfer expiration.
	scheduler *platform.Scheduler
	factory   AgentFactory

	peerChannelsFactory   *peer_channels.Factory
	peerChannelsProtocols *channels.Protocols

	lock sync.Mutex
}

type Broadcaster interface {
	BroadcastTx(context.Context, *expanded_tx.ExpandedTx, []uint32) error
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
	feeLockingScript bitcoin.Script, caches *state.Caches, balanceLocker state.BalanceLocker,
	store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher, headers BlockHeaders,
	scheduler *platform.Scheduler, factory AgentFactory,
	peerChannelsFactory *peer_channels.Factory) (*Agent, error) {

	newContract := &state.Contract{
		LockingScript: lockingScript,
	}

	contract, err := caches.Contracts.Add(ctx, newContract)
	if err != nil {
		return nil, errors.Wrap(err, "get contract")
	}

	result := &Agent{
		key:                   key,
		config:                config,
		contract:              contract,
		lockingScript:         lockingScript,
		feeLockingScript:      feeLockingScript,
		caches:                caches,
		balanceLocker:         balanceLocker,
		store:                 store,
		broadcaster:           broadcaster,
		fetcher:               fetcher,
		headers:               headers,
		scheduler:             scheduler,
		factory:               factory,
		peerChannelsFactory:   peerChannelsFactory,
		peerChannelsProtocols: channels.NewProtocols(channelsExpandedTx.NewProtocol()),
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

func (a *Agent) InRecoveryMode() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.RecoveryMode
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

func (a *Agent) BroadcastTx(ctx context.Context, etx *expanded_tx.ExpandedTx,
	indexes []uint32) error {

	a.lock.Lock()
	broadcaster := a.broadcaster
	a.lock.Unlock()

	if broadcaster == nil {
		return nil
	}

	return broadcaster.BroadcastTx(ctx, etx, indexes)
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
