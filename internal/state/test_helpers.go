package state

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
)

type TestCaches struct {
	Timeout       time.Duration
	Cache         *cacher.Cache
	Contracts     *ContractCache
	Balances      *BalanceCache
	Transactions  *TransactionCache
	Subscriptions *SubscriptionCache
	Interrupt     chan interface{}
	Complete      chan error
	Shutdown      chan error
	StartShutdown chan interface{}
	Wait          sync.WaitGroup

	failed     error
	failedLock sync.Mutex
}

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, t *testing.T, store storage.StreamStorage,
	config cacher.Config, timeout time.Duration) *TestCaches {

	result := &TestCaches{
		Timeout:       timeout,
		Cache:         cacher.NewCache(store, config),
		Interrupt:     make(chan interface{}),
		Complete:      make(chan error, 1),
		Shutdown:      make(chan error, 1),
		StartShutdown: make(chan interface{}),
	}

	var err error
	result.Contracts, err = NewContractCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract cache : %s", err))
	}

	result.Balances, err = NewBalanceCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create balance cache : %s", err))
	}

	result.Transactions, err = NewTransactionCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create transaction cache : %s", err))
	}

	result.Subscriptions, err = NewSubscriptionCache(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create subscription cache : %s", err))
	}

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Cache panic : %s", err)
				result.Complete <- fmt.Errorf("panic: %s", err)
			}

			result.Wait.Done()
		}()

		result.Wait.Add(1)
		err := result.Cache.Run(ctx, result.Interrupt, result.Shutdown)
		if err != nil {
			t.Errorf("Cache returned an error : %s", err)
		}
		result.Complete <- err
	}()

	go func() {
		select {
		case err := <-result.Shutdown:
			t.Errorf("Cache shutting down : %s", err)

			if err != nil {
				result.failedLock.Lock()
				result.failed = err
				result.failedLock.Unlock()
			}

		case err, ok := <-result.Complete:
			if ok && err != nil {
				t.Errorf("Cache failed : %s", err)
			} else {
				// StartShutdown should have been triggered first.
				t.Errorf("Cache completed prematurely")
			}

		case <-result.StartShutdown:
			t.Logf("Cache start shutdown triggered")
		}
	}()

	return result
}

func (c *TestCaches) StopTestCaches() {
	close(c.StartShutdown)
	close(c.Interrupt)
	select {
	case err := <-c.Complete:
		if err != nil {
			panic(fmt.Sprintf("Cache failed : %s", err))
		}

	case <-time.After(c.Timeout):
		panic("Cache shutdown timed out")
	}
}

func (c *TestCaches) IsFailed() error {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()

	return c.failed
}

// MockInstrument creates a contract and instrument.
// `caches.Contracts.Release(ctx, contractLockingScript)` must be called before the end of the test.
func MockInstrument(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, *Contract, *Instrument) {

	contractKey, contractLockingScript, _ := MockKey()
	adminKey, adminLockingScript, adminAddress := MockKey()

	var keyHash bitcoin.Hash32
	rand.Read(keyHash[:])

	contract := &Contract{
		KeyHash:       keyHash,
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName: "Test",
			AdminAddress: adminAddress.Bytes(),
			ContractFee:  100,
			Timestamp:    uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
	}
	rand.Read(contract.FormationTxID[:])

	currency := &instruments.Currency{
		CurrencyCode: "USD",
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	instrument := &Instrument{
		ContractHash: CalculateContractHash(contractLockingScript),
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
			// InstrumentPermissions            []byte   `protobuf:"bytes,3,opt,name=InstrumentPermissions,proto3" json:"InstrumentPermissions,omitempty"`
			// EnforcementOrdersPermitted       bool     `protobuf:"varint,6,opt,name=EnforcementOrdersPermitted,proto3" json:"EnforcementOrdersPermitted,omitempty"`
			// VotingRights                     bool     `protobuf:"varint,7,opt,name=VotingRights,proto3" json:"VotingRights,omitempty"`
			// VoteMultiplier                   uint32   `protobuf:"varint,8,opt,name=VoteMultiplier,proto3" json:"VoteMultiplier,omitempty"`
			// AdministrationProposal           bool     `protobuf:"varint,9,opt,name=AdministrationProposal,proto3" json:"AdministrationProposal,omitempty"`
			// HolderProposal                   bool     `protobuf:"varint,10,opt,name=HolderProposal,proto3" json:"HolderProposal,omitempty"`
			// InstrumentModificationGovernance uint32   `protobuf:"varint,11,opt,name=InstrumentModificationGovernance,proto3" json:"InstrumentModificationGovernance,omitempty"`
			AuthorizedTokenQty: authorizedQuantity,
			InstrumentType:     instruments.CodeCurrency,
			InstrumentPayload:  currencyBuf.Bytes(),
			// InstrumentRevision               uint32   `protobuf:"varint,15,opt,name=InstrumentRevision,proto3" json:"InstrumentRevision,omitempty"`
			Timestamp: uint64(time.Now().UnixNano()),
			// TradeRestrictions                []string `protobuf:"bytes,17,rep,name=TradeRestrictions,proto3" json:"TradeRestrictions,omitempty"`
		},
		CreationTxID: &bitcoin.Hash32{},
	}
	rand.Read(instrument.InstrumentCode[:])
	instrument.Creation.InstrumentCode = instrument.InstrumentCode[:]
	rand.Read(instrument.CreationTxID[:])
	copy(instrument.InstrumentType[:], []byte(instruments.CodeCurrency))

	contract.Instruments = append(contract.Instruments, instrument)

	var err error
	contract, err = caches.Contracts.Add(ctx, contract)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	adminBalance, err := caches.Balances.Add(ctx, contractLockingScript, instrument.InstrumentCode,
		&Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode, adminBalance)

	return contractKey, contractLockingScript, adminKey, contract, instrument
}

func MockKey() (bitcoin.Key, bitcoin.Script, bitcoin.RawAddress) {
	key, err := bitcoin.GenerateKey(bitcoin.MainNet)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate key : %s", err))
	}

	lockingScript, err := key.LockingScript()
	if err != nil {
		panic(fmt.Sprintf("Failed to create lockingScript : %s", err))
	}

	ra, err := key.RawAddress()
	if err != nil {
		panic(fmt.Sprintf("Failed to create raw address : %s", err))
	}

	return key, lockingScript, ra
}

func MockOutPoint(lockingScript bitcoin.Script, value uint64) *wire.OutPoint {
	outpoint := &wire.OutPoint{
		Index: uint32(rand.Intn(5)),
	}
	rand.Read(outpoint.Hash[:])

	return outpoint
}

type MockTxBroadcaster struct {
	txs []*wire.MsgTx

	lock sync.Mutex
}

func NewMockTxBroadcaster() *MockTxBroadcaster {
	return &MockTxBroadcaster{}
}

func (b *MockTxBroadcaster) BroadcastTx(ctx context.Context, tx *wire.MsgTx) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = append(b.txs, tx)
	return nil
}

func (b *MockTxBroadcaster) ClearTxs() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = nil
}

func (b *MockTxBroadcaster) GetLastTx() *wire.MsgTx {
	b.lock.Lock()
	defer b.lock.Unlock()

	l := len(b.txs)
	if l == 0 {
		return nil
	}

	return b.txs[l-1]
}
