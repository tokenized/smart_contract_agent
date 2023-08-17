package state

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	ci "github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

type TestCaches struct {
	Timeout         time.Duration
	Cache           ci.Cacher
	Caches          *Caches
	Locker          locker.Locker
	LockerInterrupt chan interface{}
	LockerComplete  chan error
	StartShutdown   chan interface{}
	Wait            sync.WaitGroup

	failed     error
	failedLock sync.Mutex
}

// StartTestCaches starts all the caches and wraps them into one interrupt and complete.
func StartTestCaches(ctx context.Context, t testing.TB, store storage.StreamStorage,
	timeout time.Duration) *TestCaches {

	result := &TestCaches{
		Timeout:         timeout,
		Cache:           ci.NewSimpleCache(store),
		LockerInterrupt: make(chan interface{}),
		LockerComplete:  make(chan error, 1),
		StartShutdown:   make(chan interface{}),
	}

	var err error
	result.Caches, err = NewCaches(result.Cache)
	if err != nil {
		panic(fmt.Sprintf("Failed to create caches : %s", err))
	}

	locker := locker.NewThreadedLocker(1000)

	result.Locker = locker

	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Locker panic : %s", err)
				result.LockerComplete <- fmt.Errorf("panic: %s", err)
			}

			result.Wait.Done()
			// t.Logf("Locker finished")
		}()

		result.Wait.Add(1)
		err := locker.Run(ctx, result.LockerInterrupt)
		if err != nil {
			t.Errorf("Locker returned an error : %s", err)
		}
		result.LockerComplete <- err
	}()

	go func() {
		select {
		case <-result.StartShutdown:
			// t.Logf("Cache start shutdown triggered")
		}
	}()

	return result
}

func (c *TestCaches) StopTestCaches() {
	close(c.LockerInterrupt)
	select {
	case err := <-c.LockerComplete:
		if err != nil {
			panic(fmt.Sprintf("Locker failed : %s", err))
		}

	case <-time.After(c.Timeout):
		panic("Locker shutdown timed out")
	}

	close(c.StartShutdown)
}

func (c *TestCaches) IsFailed() error {
	c.failedLock.Lock()
	defer c.failedLock.Unlock()

	return c.failed
}

func MockContract(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract) {

	contractKey, contractLockingScript, _ := MockKey()
	adminKey, adminLockingScript, adminAddress := MockKey()
	_, _, entityAddress := MockKey()

	contract := &Contract{
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName:           "Test",
			AdminAddress:           adminAddress.Bytes(),
			ContractFee:            100,
			ContractType:           actions.ContractTypeInstrument,
			EntityContract:         entityAddress.Bytes(),
			AdministrationProposal: true,
			HolderProposal:         true,
			Timestamp:              uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
	}
	rand.Read(contract.FormationTxID[:])

	addedContract, err := caches.Caches.Contracts.Add(ctx, contract)
	if err != nil {
		panic(fmt.Sprintf("Failed to add contract : %s", err))
	}

	if addedContract != contract {
		panic("Created contract is not new")
	}

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract
}

func MockInstrumentOnly(ctx context.Context, caches *TestCaches, contract *Contract) *Instrument {
	currency := &instruments.Currency{
		CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	contract.Lock()
	contractLockingScript := contract.LockingScript
	adminAddress, err := bitcoin.DecodeRawAddress(contract.Formation.AdminAddress)
	if err != nil {
		panic(fmt.Sprintf("Failed to create admin address : %s", err))
	}

	adminLockingScript, err := adminAddress.LockingScript()
	if err != nil {
		panic(fmt.Sprintf("Failed to create admin locking script : %s", err))
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract address : %s", err))
	}
	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++
	contract.Unlock()

	instrument := &Instrument{
		InstrumentCode: InstrumentCode(nextInstrumentCode),
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
			InstrumentCode: nextInstrumentCode[:],
			// InstrumentPermissions            []byte   `protobuf:"bytes,3,opt,name=InstrumentPermissions,proto3" json:"InstrumentPermissions,omitempty"`
			// EnforcementOrdersPermitted       bool     `protobuf:"varint,6,opt,name=EnforcementOrdersPermitted,proto3" json:"EnforcementOrdersPermitted,omitempty"`
			VotingRights: true,
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
	rand.Read(instrument.CreationTxID[:])
	copy(instrument.InstrumentType[:], []byte(instruments.CodeCurrency))

	addedInstrument, err := caches.Caches.Instruments.Add(ctx, contractLockingScript, instrument)
	if err != nil {
		panic(fmt.Sprintf("Failed to add instrument : %s", err))
	}

	if addedInstrument != instrument {
		panic("Created instrument is not new")
	}

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode,
		adminBalance)

	return instrument
}

// MockInstrument creates a contract and instrument.
// `caches.Contracts.Release(ctx, contractLockingScript)` must be called before the end of the test.
func MockInstrument(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract, *Instrument) {

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := MockContract(ctx,
		caches)

	currency := &instruments.Currency{
		CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	contract.Lock()
	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract address : %s", err))
	}
	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++
	contract.Unlock()

	instrument := &Instrument{
		InstrumentCode: InstrumentCode(nextInstrumentCode),
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
			InstrumentCode: nextInstrumentCode[:],
			// InstrumentPermissions            []byte   `protobuf:"bytes,3,opt,name=InstrumentPermissions,proto3" json:"InstrumentPermissions,omitempty"`
			EnforcementOrdersPermitted: true,
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

	addedInstrument, err := caches.Caches.Instruments.Add(ctx, contractLockingScript, instrument)
	if err != nil {
		panic(fmt.Sprintf("Failed to add instrument : %s", err))
	}

	if addedInstrument != instrument {
		panic("Created instrument is not new")
	}

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode,
		adminBalance)

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument
}

func MockInstrumentCreditNote(ctx context.Context,
	caches *TestCaches) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract, *Instrument) {

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := MockContract(ctx,
		caches)

	creditNote := &instruments.CreditNote{
		// Name: "USD Note", // deprecated
		FaceValue: &instruments.FixedCurrencyValueField{
			Value:        1,
			CurrencyCode: instruments.CurrenciesUnitedStatesDollar,
			Precision:    2,
		},
	}

	creditNoteBuf := &bytes.Buffer{}
	if err := creditNote.Serialize(creditNoteBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	authorizedQuantity := uint64(1000000)

	contract.Lock()
	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		panic(fmt.Sprintf("Failed to create contract address : %s", err))
	}
	nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
		contract.InstrumentCount)
	contract.InstrumentCount++
	contract.Unlock()

	instrument := &Instrument{
		InstrumentCode: InstrumentCode(nextInstrumentCode),
		Creation: &actions.InstrumentCreation{
			// InstrumentIndex                  uint64   `protobuf:"varint,2,opt,name=InstrumentIndex,proto3" json:"InstrumentIndex,omitempty"`
			InstrumentCode: nextInstrumentCode[:],
			// InstrumentPermissions            []byte   `protobuf:"bytes,3,opt,name=InstrumentPermissions,proto3" json:"InstrumentPermissions,omitempty"`
			EnforcementOrdersPermitted: true,
			// VotingRights                     bool     `protobuf:"varint,7,opt,name=VotingRights,proto3" json:"VotingRights,omitempty"`
			// VoteMultiplier                   uint32   `protobuf:"varint,8,opt,name=VoteMultiplier,proto3" json:"VoteMultiplier,omitempty"`
			// AdministrationProposal           bool     `protobuf:"varint,9,opt,name=AdministrationProposal,proto3" json:"AdministrationProposal,omitempty"`
			// HolderProposal                   bool     `protobuf:"varint,10,opt,name=HolderProposal,proto3" json:"HolderProposal,omitempty"`
			// InstrumentModificationGovernance uint32   `protobuf:"varint,11,opt,name=InstrumentModificationGovernance,proto3" json:"InstrumentModificationGovernance,omitempty"`
			AuthorizedTokenQty: authorizedQuantity,
			InstrumentType:     instruments.CodeCreditNote,
			InstrumentPayload:  creditNoteBuf.Bytes(),
			// InstrumentRevision               uint32   `protobuf:"varint,15,opt,name=InstrumentRevision,proto3" json:"InstrumentRevision,omitempty"`
			Timestamp: uint64(time.Now().UnixNano()),
			// TradeRestrictions                []string `protobuf:"bytes,17,rep,name=TradeRestrictions,proto3" json:"TradeRestrictions,omitempty"`
		},
		CreationTxID: &bitcoin.Hash32{},
	}
	rand.Read(instrument.InstrumentCode[:])
	instrument.Creation.InstrumentCode = instrument.InstrumentCode[:]
	rand.Read(instrument.CreationTxID[:])
	copy(instrument.InstrumentType[:], []byte(instruments.CodeCreditNote))

	addedInstrument, err := caches.Caches.Instruments.Add(ctx, contractLockingScript, instrument)
	if err != nil {
		panic(fmt.Sprintf("Failed to add instrument : %s", err))
	}

	if addedInstrument != instrument {
		panic("Created instrument is not new")
	}

	adminBalance, err := caches.Caches.Balances.Add(ctx, contractLockingScript,
		instrument.InstrumentCode, &Balance{
			LockingScript: adminLockingScript,
			Quantity:      authorizedQuantity,
			Timestamp:     instrument.Creation.Timestamp,
			TxID:          instrument.CreationTxID,
		})
	if err != nil {
		panic(fmt.Sprintf("Failed to add admin balance : %s", err))
	}

	caches.Caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode,
		adminBalance)

	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument
}

func MockContractWithVoteSystems(ctx context.Context, caches *TestCaches,
	votingSystems []*actions.VotingSystemField) (bitcoin.Key, bitcoin.Script, bitcoin.Key, bitcoin.Script, *Contract) {

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := MockContract(ctx, caches)

	contract.Formation.VotingSystems = votingSystems

	// Set all fields to be updatable by an administration proposal using these voting systems.
	permissions := permissions.Permissions{
		permissions.Permission{
			Permitted:              false, // Issuer can update field without proposal
			AdministrationProposal: true,  // Issuer can update field with a proposal
			HolderProposal:         false, // Holder's can initiate proposals to update field
		},
	}

	permissions[0].VotingSystemsAllowed = make([]bool, len(votingSystems))
	for i := range permissions[0].VotingSystemsAllowed {
		permissions[0].VotingSystemsAllowed[i] = true // Enable this voting system for proposals on this field.
	}

	permissionsBytes, err := permissions.Bytes()
	if err != nil {
		panic(fmt.Sprintf("Failed to serialize contract permissions : %s", err))
	}
	contract.Formation.ContractPermissions = permissionsBytes

	contract.MarkModified()
	return contractKey, contractLockingScript, adminKey, adminLockingScript, contract
}

type MockBalance struct {
	Key           bitcoin.Key
	LockingScript bitcoin.Script
	Quantity      uint64
}

func MockBalances(ctx context.Context, caches *TestCaches, contract *Contract,
	instrument *Instrument, count int) []*MockBalance {

	balances := make([]*MockBalance, count)
	balancesToAdd := make(Balances, count)
	for i := 0; i < count; i++ {
		key, lockingScript, _ := MockKey()
		quantity := uint64(rand.Intn(1000))

		balance := &Balance{
			LockingScript: lockingScript,
			Quantity:      quantity,
			Timestamp:     uint64(time.Now().UnixNano()),
			TxID:          &bitcoin.Hash32{},
		}
		rand.Read(balance.TxID[:])

		balances[i] = &MockBalance{
			Key:           key,
			LockingScript: lockingScript,
			Quantity:      quantity,
		}

		balancesToAdd[i] = balance
	}

	contract.Lock()
	contractLockingScript := contract.LockingScript
	contract.Unlock()

	instrument.Lock()
	instrumentCode := instrument.InstrumentCode
	instrument.Unlock()

	addedBalances, err := caches.Caches.Balances.AddMulti(ctx, contractLockingScript,
		instrumentCode, balancesToAdd)
	if err != nil {
		panic(fmt.Sprintf("Failed to add balances : %s", err))
	}

	if err := caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode,
		addedBalances); err != nil {
		panic(fmt.Sprintf("Failed to release balances : %s", err))
	}

	return balances
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

func MockOutPointTx(lockingScript bitcoin.Script, value uint64) (*wire.MsgTx, *wire.OutPoint) {
	tx := wire.NewMsgTx(1)

	for i := 0; i < rand.Intn(3); i++ {
		tx.AddTxOut(wire.NewTxOut(uint64(rand.Intn(1000)+1), nil))
	}

	index := len(tx.TxOut)
	tx.AddTxOut(wire.NewTxOut(value, lockingScript))

	for i := 0; i < rand.Intn(3); i++ {
		tx.AddTxOut(wire.NewTxOut(uint64(rand.Intn(1000)+1), nil))
	}

	outpoint := &wire.OutPoint{
		Hash:  *tx.TxHash(),
		Index: uint32(index),
	}

	return tx, outpoint
}

type MockTxBroadcaster struct {
	txs []*expanded_tx.ExpandedTx

	lock sync.Mutex
}

func NewMockTxBroadcaster() *MockTxBroadcaster {
	return &MockTxBroadcaster{}
}

func (b *MockTxBroadcaster) BroadcastTx(ctx context.Context, etx *expanded_tx.ExpandedTx,
	indexes []uint32) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = append(b.txs, etx)
	return nil
}

func (b *MockTxBroadcaster) ClearTxs() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.txs = nil
}

func (b *MockTxBroadcaster) GetLastTx() *expanded_tx.ExpandedTx {
	b.lock.Lock()
	defer b.lock.Unlock()

	l := len(b.txs)
	if l == 0 {
		return nil
	}

	return b.txs[l-1]
}

type MockHeaders struct {
	hashes  map[int]*bitcoin.Hash32
	headers map[int]*wire.BlockHeader

	lock sync.Mutex
}

func NewMockHeaders() *MockHeaders {
	return &MockHeaders{
		hashes:  make(map[int]*bitcoin.Hash32),
		headers: make(map[int]*wire.BlockHeader),
	}
}

func (h *MockHeaders) AddHash(height int, hash bitcoin.Hash32) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.hashes[height] = &hash
}

func (h *MockHeaders) AddHeader(height int, header *wire.BlockHeader) {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.headers[height] = header
}

func (h *MockHeaders) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	hash, exists := h.hashes[height]
	if exists {
		return hash, nil
	}

	return nil, nil
}

func (h *MockHeaders) GetHeader(ctx context.Context, height int) (*wire.BlockHeader, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	header, exists := h.headers[height]
	if exists {
		return header, nil
	}

	return nil, nil
}
