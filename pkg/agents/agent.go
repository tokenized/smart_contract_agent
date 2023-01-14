package agents

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/contract_services"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

var (
	ErrNotRelevant = errors.New("Not Relevant")

	ErrNotImplemented = errors.New("Not Implemented")

	agentDataVersion = uint8(0)
	endian           = binary.LittleEndian
)

type Config struct {
	IsTest                  bool            `default:"true" envconfig:"IS_TEST" json:"is_test"`
	FeeRate                 float32         `default:"0.05" envconfig:"FEE_RATE" json:"fee_rate"`
	DustFeeRate             float32         `default:"0.0" envconfig:"DUST_FEE_RATE" json:"dust_fee_rate"`
	MinFeeRate              float32         `default:"0.05" envconfig:"MIN_FEE_RATE" json:"min_fee_rate"`
	MultiContractExpiration config.Duration `default:"10s" envconfig:"MULTI_CONTRACT_EXPIRATION" json:"multi_contract_expiration"`
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

type AgentData struct {
	Key           bitcoin.Key    `bsor:"1" envconfig:"KEY" json:"key" masked:"true"`
	LockingScript bitcoin.Script `bsor:"2" envconfig:"LOCKING_SCRIPT" json:"locking_script"`

	ContractFee      uint64         `bsor:"3" envconfig:"CONTRACT_FEE" json:"contract_fee"`
	FeeLockingScript bitcoin.Script `bsor:"4" envconfig:"FEE_LOCKING_SCRIPT" json:"fee_locking_script"`

	RequestPeerChannel *peer_channels.Channel `bsor:"5" envconfig:"REQUEST_PEER_CHANNEL" json:"request_peer_channel" masked:"true"`

	AdminLockingScript bitcoin.Script `bsor:"6" envconfig:"ADMIN_LOCKING_SCRIPT" json:"admin_locking_script"`

	IsActive bool `bsor:"7" envconfig:"IS_ACTIVE" json:"is_active"`
}

type Agent struct {
	data     AgentData
	dataLock sync.Mutex

	config atomic.Value

	contract *state.Contract

	store        storage.CopyList
	caches       *state.Caches
	transactions *transactions.TransactionCache
	services     *contract_services.ContractServicesCache
	locker       locker.Locker

	broadcaster Broadcaster
	fetcher     Fetcher
	headers     BlockHeaders

	peerChannelResponses chan PeerChannelResponse

	// scheduler and store are used to schedule tasks that spawn a new agent and perform a
	// function. For example, vote finalization and multi-contract transfer expiration.
	scheduler  *scheduler.Scheduler
	agentStore Store

	peerChannelsFactory *peer_channels.Factory

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

type Store interface {
	GetAgent(ctx context.Context, lockingScript bitcoin.Script) (*Agent, error)
}

func NewAgent(ctx context.Context, data AgentData, config Config, caches *state.Caches,
	transactions *transactions.TransactionCache, services *contract_services.ContractServicesCache,
	locker locker.Locker, store storage.CopyList, broadcaster Broadcaster, fetcher Fetcher,
	headers BlockHeaders, scheduler *scheduler.Scheduler, agentStore Store,
	peerChannelsFactory *peer_channels.Factory,
	peerChannelResponses chan PeerChannelResponse) (*Agent, error) {

	newContract := &state.Contract{
		LockingScript: data.LockingScript,
	}

	contract, err := caches.Contracts.Add(ctx, newContract)
	if err != nil {
		return nil, errors.Wrap(err, "get contract")
	}

	result := &Agent{
		data:                 data,
		contract:             contract,
		caches:               caches,
		transactions:         transactions,
		services:             services,
		locker:               locker,
		store:                store,
		broadcaster:          broadcaster,
		fetcher:              fetcher,
		headers:              headers,
		scheduler:            scheduler,
		agentStore:           agentStore,
		peerChannelsFactory:  peerChannelsFactory,
		peerChannelResponses: peerChannelResponses,
	}

	result.config.Store(config)

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

func (a *Agent) Key() bitcoin.Key {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.Key.Copy()
}

func (a *Agent) LockingScript() bitcoin.Script {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.LockingScript.Copy()
}

func (a *Agent) ContractHash() state.ContractHash {
	return state.CalculateContractHash(a.LockingScript())
}

func (a *Agent) ContractFee() uint64 {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.ContractFee
}

func (a *Agent) FeeLockingScript() bitcoin.Script {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.FeeLockingScript.Copy()
}

func (a *Agent) RequestPeerChannel() *peer_channels.Channel {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	if a.data.RequestPeerChannel == nil {
		return nil
	}

	c := a.data.RequestPeerChannel.Copy()
	return &c
}

func (a *Agent) AdminLockingScript() bitcoin.Script {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.AdminLockingScript.Copy()
}

func (a *Agent) SetAdminLockingScript(lockingScript bitcoin.Script) {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	a.data.AdminLockingScript = lockingScript.Copy()
}

func (a *Agent) IsActive() bool {
	a.dataLock.Lock()
	defer a.dataLock.Unlock()

	return a.data.IsActive
}

func (a *Agent) Contract() *state.Contract {
	return a.contract
}

func (a *Agent) CheckContractIsAvailable(now uint64) error {
	a.contract.Lock()
	defer a.contract.Unlock()

	return a.contract.CheckIsAvailable(now)
}

func (a *Agent) Config() Config {
	return a.config.Load().(Config)
}

func (a *Agent) SetConfig(config Config) {
	a.config.Store(config)
}

func (a *Agent) Now() uint64 {
	return uint64(time.Now().UnixNano())
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

func (a *AgentData) Serialize(w io.Writer) error {
	bs, err := bsor.MarshalBinary(a)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, agentDataVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(bs))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(bs); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (a *AgentData) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	bs := make([]byte, size)
	if _, err := io.ReadFull(r, bs); err != nil {
		return errors.Wrap(err, "read")
	}

	if _, err := bsor.UnmarshalBinary(bs, a); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
