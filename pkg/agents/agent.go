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

func (a *Agent) InRecoveryMode() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.config.RecoveryMode
}

func (a *Agent) addRecoveryRequests(ctx context.Context, txid bitcoin.Hash32,
	requestActions []Action) (bool, error) {

	if len(requestActions) == 0 {
		return false, nil // no requests
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.config.RecoveryMode {
		return false, nil
	}

	recoveryTx := &state.RecoveryTransaction{
		TxID:          txid,
		OutputIndexes: make([]int, len(requestActions)),
	}

	for i, action := range requestActions {
		recoveryTx.OutputIndexes[i] = action.OutputIndex
	}

	newRecoveryTxs := &state.RecoveryTransactions{
		Transactions: []*state.RecoveryTransaction{recoveryTx},
	}

	recoveryTxs, err := a.caches.RecoveryTransactions.Add(ctx, a.lockingScript, newRecoveryTxs)
	if err != nil {
		return false, errors.Wrap(err, "get recovery txs")
	}
	defer a.caches.RecoveryTransactions.Release(ctx, a.lockingScript)

	if recoveryTxs != newRecoveryTxs {
		recoveryTxs.Lock()
		recoveryTxs.Append(recoveryTx)
		recoveryTxs.Unlock()
	}

	return true, nil
}

func (a *Agent) removeRecoveryRequest(ctx context.Context, txid bitcoin.Hash32,
	outputIndex int) (bool, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.config.RecoveryMode {
		return false, nil
	}

	recoveryTxs, err := a.caches.RecoveryTransactions.Get(ctx, a.lockingScript)
	if err != nil {
		return false, errors.Wrap(err, "get recovery txs")
	}

	if recoveryTxs == nil {
		return false, nil
	}
	defer a.caches.RecoveryTransactions.Release(ctx, a.lockingScript)

	recoveryTxs.Lock()
	result := recoveryTxs.RemoveOutput(txid, outputIndex)
	recoveryTxs.Unlock()

	if result {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("request_txid", txid),
			logger.Int("request_output_index", outputIndex),
		}, "Removed recovery request")
	}

	return result, nil
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

func isRequest(action actions.Action) bool {
	switch action.(type) {
	case *actions.ContractOffer, *actions.ContractAmendment, *actions.ContractAddressChange:
		return true

	case *actions.BodyOfAgreementOffer, *actions.BodyOfAgreementAmendment:
		return true

	case *actions.InstrumentDefinition, *actions.InstrumentModification:
		return true

	case *actions.Transfer:
		return true

	case *actions.Proposal, *actions.BallotCast:
		return true

	case *actions.Order:
		return true

	case *actions.Message:
		return true

	default:
		return false
	}
}

func containsRequest(actions []Action) bool {
	for _, action := range actions {
		if isRequest(action.Action) {
			return true
		}
	}

	return false
}

func (a *Agent) ProcessRecoveryRequests(ctx context.Context, now uint64) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("recovery", uuid.New()))

	agentLockingScript := a.LockingScript()
	recoveryTxs, err := a.caches.RecoveryTransactions.Get(ctx, agentLockingScript)
	if err != nil {
		return errors.Wrap(err, "get recovery txs")
	}

	if recoveryTxs == nil {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "No recovery requests to process")
		return nil
	}
	defer a.caches.RecoveryTransactions.Release(ctx, agentLockingScript)

	recoveryTxs.Lock()
	copyTxs := recoveryTxs.Copy()
	recoveryTxs.Unlock()

	if len(copyTxs.Transactions) == 0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "No recovery requests to process")
		return nil
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("contract_locking_script", agentLockingScript),
		logger.Int("request_count", len(copyTxs.Transactions)),
	}, "Processing recovery requests")

	for _, request := range copyTxs.Transactions {
		if err := a.processRecoveryRequest(ctx, request, now); err != nil {
			return errors.Wrapf(err, "process recovery request: %s", request.TxID)
		}

		recoveryTxs.Lock()
		recoveryTxs.Remove(request.TxID)
		recoveryTxs.Unlock()
	}

	return nil
}

func (a *Agent) processRecoveryRequest(ctx context.Context, request *state.RecoveryTransaction,
	now uint64) error {

	transaction, err := a.caches.Transactions.Get(ctx, request.TxID)
	if err != nil {
		return errors.Wrap(err, "get tx")
	}

	if transaction == nil {
		return errors.New("Transaction Not Found")
	}
	defer a.caches.Transactions.Release(ctx, request.TxID)

	agentLockingScript := a.LockingScript()
	isTest := a.IsTest()
	actionList, err := compileActions(transaction, agentLockingScript, isTest)
	if err != nil {
		return errors.Wrap(err, "compile tx")
	}

	var recoveryActions []Action
	for _, outputIndex := range request.OutputIndexes {
		found := false
		for _, action := range actionList {
			if outputIndex == action.OutputIndex {
				found = true
				recoveryActions = append(recoveryActions, action)
				break
			}
		}

		if !found {
			return fmt.Errorf("Output action %d not found", outputIndex)
		}
	}

	if err := a.Process(ctx, transaction, recoveryActions, now); err != nil {
		return errors.Wrap(err, "process")
	}

	return nil
}

func (a *Agent) Process(ctx context.Context, transaction *state.Transaction,
	actionList []Action, now uint64) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()

	var requestActions []Action
	var responseActions []Action
	for _, action := range actionList {
		if isRequest(action.Action) {
			requestActions = append(requestActions, action)
		} else {
			responseActions = append(responseActions, action)
		}
	}

	if inRecovery, err := a.addRecoveryRequests(ctx, txid, requestActions); err != nil {
		return errors.Wrap(err, "recovery request")
	} else if inRecovery {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", txid),
			logger.Stringer("contract_locking_script", agentLockingScript),
		}, "Saving transaction requests for recovery")

		if len(responseActions) == 0 {
			return nil
		}

		// Process only the responses
		actionList = responseActions
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing transaction")

	var feeRate, minFeeRate float32
	if containsRequest(actionList) {
		transaction.Lock()
		fee, err := transaction.CalculateFee()
		if err != nil {
			transaction.Unlock()
			return errors.Wrap(err, "calculate fee")
		}
		size := transaction.Size()
		transaction.Unlock()

		minFeeRate = a.MinFeeRate()
		feeRate = float32(fee) / float32(size)
	}

	for i, action := range actionList {
		if isRequest(action.Action) {
			if feeRate < minFeeRate {
				return errors.Wrap(a.sendRejection(ctx, transaction, action.OutputIndex,
					platform.NewRejectError(actions.RejectionsInsufficientTxFeeFunding,
						fmt.Sprintf("fee rate %.4f, minimum %.4f", feeRate, minFeeRate)), now),
					"reject")
			}
		}

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

// ProcessUnsafe performs actions to resolve unsafe or double spent tx.
func (a *Agent) ProcessUnsafe(ctx context.Context, transaction *state.Transaction,
	actionList []Action, now uint64) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("trace", uuid.New()))

	txid := transaction.GetTxID()
	agentLockingScript := a.LockingScript()
	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("txid", txid),
		logger.Stringer("contract_locking_script", agentLockingScript),
	}, "Processing unsafe transaction")

	for i, action := range actionList {
		if isRequest(action.Action) {
			return errors.Wrap(a.sendRejection(ctx, transaction, action.OutputIndex,
				platform.NewRejectError(actions.RejectionsDoubleSpend, ""), now), "reject")
		}

		// If it isn't a request then we can process it like normal.
		if err := a.processAction(ctx, transaction, txid, action.Action, action.OutputIndex,
			now); err != nil {
			return errors.Wrapf(err, "process action %d: %s", i, action.Action.Code())
		}
	}

	return nil
}
