package agents

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_interpreter"
	"github.com/tokenized/bitcoin_interpreter/agent_bitcoin_transfer"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/json"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"
)

func Test_Transfers_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Basic(ctx, t, store, cache)
}

func Test_Transfers_TransferFee_Success(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_TransferFee_Success(ctx, t, store, cache)
}

func Test_Transfers_TransferFee_Reject(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_TransferFee_Reject(ctx, t, store, cache)
}

func Test_Transfers_Multi_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_Basic(ctx, t, store, cache, 100, time.Millisecond)

	if !cache.IsEmpty(ctx) {
		t.Fatalf("Cache not empty")
	}
}

func Test_Transfers_Multi_TransferFee_Success(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_TransferFee_Success(ctx, t, store, cache, 100, time.Millisecond)

	if !cache.IsEmpty(ctx) {
		t.Fatalf("Cache not empty")
	}
}

func Test_Transfers_Multi_TransferFee_Reject_First(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_TransferFee_Reject_First(ctx, t, store, cache, 100, time.Millisecond)

	if !cache.IsEmpty(ctx) {
		t.Fatalf("Cache not empty")
	}
}

func Test_Transfers_Multi_TransferFee_Reject_Second(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_TransferFee_Reject_Second(ctx, t, store, cache, 100, time.Millisecond)

	if !cache.IsEmpty(ctx) {
		t.Fatalf("Cache not empty")
	}
}

func Test_Transfers_Multi_Expire(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_Expire(ctx, t, store, cache)

	if !cache.IsEmpty(ctx) {
		remaining := cache.ListCached(ctx)
		for _, path := range remaining {
			println(path)
		}
		t.Fatalf("Cache is not empty")
	}
}

func Test_Transfers_Multi_Reject_First(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_Reject_First(ctx, t, store, cache)

	if !cache.IsEmpty(ctx) {
		remaining := cache.ListCached(ctx)
		for _, path := range remaining {
			println(path)
		}
		t.Fatalf("Cache is not empty")
	}
}

func Test_Transfers_Multi_Reject_Second(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	RunTest_Transfers_Multi_Reject_Second(ctx, t, store, cache)

	if !cache.IsEmpty(ctx) {
		remaining := cache.ListCached(ctx)
		for _, path := range remaining {
			println(path)
		}
		t.Fatalf("Cache is not empty")
	}
}

// Test_Transfers_InsufficientQuantity creates a transfer action for locking scripts that don't have
// any tokens and will be rejected for insufficient quantity.
func Test_Transfers_InsufficientQuantity(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	var keys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	var lockingScripts []bitcoin.Script
	var resultQuantities []uint64

	// Add senders
	senderCount := mathRand.Intn(5) + 1
	mockBalances := state.MockBalances(ctx, &test.Caches.TestCaches, test.Contract,
		test.Instrument, senderCount)
	senderQuantity := uint64(0)
	for s := 0; s < senderCount; s++ {
		mockBalance := mockBalances[s]
		quantity := mockBalance.Quantity
		resultQuantities = append(resultQuantities, quantity)
		if s == 0 {
			quantity++ // add extra quantity that isn't available
		}

		senderQuantity += quantity
		// Add sender
		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		keys = append(keys, mockBalance.Keys...)
		lockingScripts = append(lockingScripts, mockBalance.LockingScript)

		outpoint := state.MockOutPoint(mockBalance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: mockBalance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, mockBalance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	// Add receivers
	receiverCount := 0
	for {
		receiverCount++
		_, lockingScript, ra := state.MockKey()
		lockingScripts = append(lockingScripts, lockingScript)
		resultQuantities = append(resultQuantities, 0)
		var quantity uint64
		if receiverCount == 100 {
			// max out at 100 receivers
			quantity = senderQuantity
			senderQuantity = 0
		} else {
			quantity = uint64(mathRand.Intn(100000000)) + 1
			if quantity > senderQuantity {
				quantity = senderQuantity
			}
			senderQuantity -= quantity
		}
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		if senderQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 100, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint := state.MockOutPoint(lockingScript, 500)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: lockingScript,
		Value:         500,
	})

	if err := tx.AddInput(*outpoint, lockingScript, 500); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	balances, err := test.Caches.TestCaches.Caches.Balances.GetMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, lockingScripts)
	if err != nil {
		t.Fatalf("Failed to get balances : %s", err)
	}

	for i, balance := range balances {
		js, _ := json.MarshalIndent(balance, "", "  ")
		t.Logf("Balance : %s", js)

		balance.Lock()
		gotQuantity := balance.SettlePendingQuantity()

		if gotQuantity != resultQuantities[i] {
			t.Errorf("Balance wrong : got %d, want %d", gotQuantity, resultQuantities[i])
		}
		balance.Unlock()
	}

	if err := test.Caches.TestCaches.Caches.Balances.ReleaseMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, balances); err != nil {
		t.Fatalf("Failed to release balances : %s", err)
	}

	time.Sleep(time.Millisecond)

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), test.Instrument.InstrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction = stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)

	responseTx := test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find rejection action
	var rejection *actions.Rejection
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		r, ok := action.(*actions.Rejection)
		if ok {
			rejection = r
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	js, _ = json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	if rejection.RejectionCode != actions.RejectionsInsufficientQuantity {
		t.Errorf("Wrong rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsInsufficientQuantity)
	}
}

// Test_Transfers_NoQuantity creates a transfer action for locking scripts that don't have any
// tokens and will be rejected for insufficient quantity.
func Test_Transfers_NoQuantity(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	var keys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	var lockingScripts []bitcoin.Script

	// Add senders
	senderCount := mathRand.Intn(5) + 1
	senderQuantity := uint64(0)
	for s := 0; s < senderCount; s++ {
		quantity := uint64(mathRand.Intn(1000))
		senderQuantity += quantity
		// Add sender
		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		key, lockingScript, _ := state.MockKey()
		keys = append(keys, key)
		lockingScripts = append(lockingScripts, lockingScript)

		outpoint := state.MockOutPoint(lockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: lockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, lockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	}

	// Add receivers
	for {
		_, lockingScript, ra := state.MockKey()
		lockingScripts = append(lockingScripts, lockingScript)
		quantity := uint64(mathRand.Intn(1000)) + 1
		if quantity > senderQuantity {
			quantity = senderQuantity
		}
		senderQuantity -= quantity
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		if senderQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint := state.MockOutPoint(lockingScript, 250)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: lockingScript,
		Value:         250,
	})

	if err := tx.AddInput(*outpoint, lockingScript, 250); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	balances, err := test.Caches.TestCaches.Caches.Balances.GetMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, lockingScripts)
	if err != nil {
		t.Fatalf("Failed to get balances : %s", err)
	}

	for _, balance := range balances {
		js, _ := json.MarshalIndent(balance, "", "  ")
		t.Logf("Balance : %s", js)

		balance.Lock()
		if balance.SettlePendingQuantity() != 0 {
			t.Errorf("Balance not zero : %d", balance.SettlePendingQuantity())
		}
		balance.Unlock()
	}

	if err := test.Caches.TestCaches.Caches.Balances.ReleaseMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, balances); err != nil {
		t.Fatalf("Failed to release balances : %s", err)
	}

	time.Sleep(time.Millisecond) // wait for stats to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing contract statistics")
	}

	stats.Lock()

	statAction := stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), test.Instrument.InstrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing instrument statistics")
	}

	stats.Lock()

	statAction = stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)

	responseTx := test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find rejection action
	var rejection *actions.Rejection
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		r, ok := action.(*actions.Rejection)
		if ok {
			rejection = r
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	js, _ = json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	if rejection.RejectionCode != actions.RejectionsInsufficientQuantity {
		t.Errorf("Wrong rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsInsufficientQuantity)
	}
}

func Test_Transfers_IdentityOracle_MissingSignature(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrumentWithOracle(ctx, t)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.AdminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.AdminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	for {
		_, _, ra := state.MockKey()
		quantity := uint64(mathRand.Intn(1000)) + 1
		if quantity > senderQuantity {
			quantity = senderQuantity
		}
		senderQuantity -= quantity
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		if senderQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint = state.MockOutPoint(lockingScript, 200)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: lockingScript,
		Value:         200,
	})

	if err := tx.AddInput(*outpoint, lockingScript, 200); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	time.Sleep(time.Millisecond) // wait for stats to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing contract statistics")
	}

	stats.Lock()

	statAction := stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), test.Instrument.InstrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing instrument statistics")
	}

	stats.Lock()

	statAction = stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)

	responseTx := test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find rejection action
	var rejection *actions.Rejection
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		r, ok := action.(*actions.Rejection)
		if ok {
			rejection = r
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	js, _ = json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	if rejection.RejectionCode != actions.RejectionsInvalidSignature {
		t.Errorf("Wrong rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsInvalidSignature)
	}
}

func Test_Transfers_IdentityOracle_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrumentWithOracle(ctx, t)

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to create contract address : %s", err)
	}

	headerHeight := 100045
	var headerHash bitcoin.Hash32
	rand.Read(headerHash[:])
	test.headers.AddHash(headerHeight, headerHash)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.AdminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1000
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.AdminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	for {
		_, _, receiverAddress := state.MockKey()

		sigHash, err := protocol.TransferOracleSigHash(ctx, contractAddress,
			test.Instrument.InstrumentCode[:], receiverAddress, headerHash, 0, 1)
		if err != nil {
			t.Fatalf("Failed to create identity sig hash : %s", err)
		}

		identitySignature, err := test.oracleKey.Sign(*sigHash)
		if err != nil {
			t.Fatalf("Failed to sign identity certificate : %s", err)
		}

		quantity := uint64(mathRand.Intn(1000)) + 1
		if quantity > senderQuantity {
			quantity = senderQuantity
		}
		senderQuantity -= quantity
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:               receiverAddress.Bytes(),
				Quantity:              quantity,
				OracleSigAlgorithm:    1,
				OracleIndex:           0,
				OracleConfirmationSig: identitySignature.Bytes(),
				OracleSigBlockHeight:  uint32(headerHeight),
				OracleSigExpiry:       0,
			})

		if senderQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint = state.MockOutPoint(lockingScript, 350)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: lockingScript,
		Value:         350,
	})

	if err := tx.AddInput(*outpoint, lockingScript, 350); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	time.Sleep(time.Millisecond) // wait for stats to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing contract statistics")
	}

	stats.Lock()

	statAction := stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), test.Instrument.InstrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing instrument statistics")
	}

	stats.Lock()

	statAction = stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)

	responseTx := test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find settlement action
	var settlement *actions.Settlement
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if s, ok := action.(*actions.Settlement); ok {
			settlement = s
		}
	}

	if settlement == nil {
		t.Fatalf("Missing settlement action")
	}

	js, _ = json.MarshalIndent(settlement, "", "  ")
	t.Logf("Settlement : %s", js)
}

func Test_Transfers_IdentityOracle_BadSignature(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrumentWithOracle(ctx, t)

	badIdentityKey, _, _ := state.MockKey()

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to create contract address : %s", err)
	}

	headerHeight := 100045
	var headerHash bitcoin.Hash32
	rand.Read(headerHash[:])
	test.headers.AddHash(headerHeight, headerHash)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.AdminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1000
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.AdminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	for {
		_, _, receiverAddress := state.MockKey()

		sigHash, err := protocol.TransferOracleSigHash(ctx, contractAddress,
			test.Instrument.InstrumentCode[:], receiverAddress, headerHash, 0, 1)
		if err != nil {
			t.Fatalf("Failed to create identity sig hash : %s", err)
		}

		identitySignature, err := badIdentityKey.Sign(*sigHash)
		if err != nil {
			t.Fatalf("Failed to sign identity certificate : %s", err)
		}

		quantity := uint64(mathRand.Intn(1000)) + 1
		if quantity > senderQuantity {
			quantity = senderQuantity
		}
		senderQuantity -= quantity
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:               receiverAddress.Bytes(),
				Quantity:              quantity,
				OracleSigAlgorithm:    1,
				OracleIndex:           0,
				OracleConfirmationSig: identitySignature.Bytes(),
				OracleSigBlockHeight:  uint32(headerHeight),
				OracleSigExpiry:       0,
			})

		if senderQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint = state.MockOutPoint(lockingScript, 300)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: lockingScript,
		Value:         300,
	})

	if err := tx.AddInput(*outpoint, lockingScript, 300); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	time.Sleep(time.Millisecond) // wait for stats to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing contract statistics")
	}

	stats.Lock()

	statAction := stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), test.Instrument.InstrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	if stats == nil {
		t.Fatalf("Missing instrument statistics")
	}

	stats.Lock()

	statAction = stats.GetAction(actions.CodeTransfer)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)

	responseTx := test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find rejection action
	var rejection *actions.Rejection
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		r, ok := action.(*actions.Rejection)
		if ok {
			rejection = r
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	js, _ = json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	if rejection.RejectionCode != actions.RejectionsInvalidSignature {
		t.Errorf("Wrong rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsInvalidSignature)
	}
}

func Test_Transfers_Bitcoin_Refund(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	feeRate := 0.05
	broadcaster := state.NewMockTxBroadcaster()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract1, instrument := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	bitcoinKey, bitcoinLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:                contractKey,
		LockingScript:      contractLockingScript,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses, test.Statistics.Add)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	fundValue := uint64(1500)

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 10; i++ {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(instrument.InstrumentType[:]),
			InstrumentCode: instrument.InstrumentCode[:],
		}

		bitcoinTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: protocol.BSVInstrumentID,
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{
				instrumentTransfer,
				bitcoinTransfer,
			},
		}

		tx := txbuilder.NewTxBuilder(float32(feeRate), 0.0)

		var spentOutputs []*expanded_tx.Output
		var inputLockingScripts []bitcoin.Script

		// Add admin as sender
		instrumentQuantity := uint64(mathRand.Intn(1000)) + 1
		receiver1Quantities = append(receiver1Quantities, instrumentQuantity)

		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: instrumentQuantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		outpoint1 := state.MockOutPoint(adminLockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: adminLockingScript,
			Value:         1,
		})
		inputLockingScripts = append(inputLockingScripts, adminLockingScript)

		if err := tx.AddInput(*outpoint1, adminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		bitcoinQuantity := uint64(mathRand.Intn(1000)) + 1
		receiver2Quantities = append(receiver2Quantities, bitcoinQuantity)

		bitcoinTransfer.InstrumentSenders = append(bitcoinTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: bitcoinQuantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		outpoint2 := state.MockOutPoint(bitcoinLockingScript, bitcoinQuantity+5)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: bitcoinLockingScript,
			Value:         bitcoinQuantity + 5,
		})
		inputLockingScripts = append(inputLockingScripts, bitcoinLockingScript)

		if err := tx.AddInput(*outpoint2, bitcoinLockingScript, bitcoinQuantity+5); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add receivers
		key, lockingScript, ra := state.MockKey()
		receiver1Keys = append(receiver1Keys, key)
		receiver1LockingScripts = append(receiver1LockingScripts, lockingScript)

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: instrumentQuantity,
			})

		key, lockingScript, ra = state.MockKey()
		receiver2Keys = append(receiver2Keys, key)
		receiver2LockingScripts = append(receiver2LockingScripts, lockingScript)

		bitcoinTransfer.InstrumentReceivers = append(bitcoinTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: bitcoinQuantity + 1,
			})

		agentBitcoinTransferLockingScript, err := agent_bitcoin_transfer.CreateScript(contractLockingScript,
			lockingScript, bitcoinLockingScript, bitcoinQuantity, bitcoinLockingScript,
			uint32(time.Now().Unix()+100))
		if err != nil {
			t.Fatalf("Failed to create agent bitcoin transfer locking script : %s", err)
		}

		// Add agent bitcoin transfer output
		if err := tx.AddOutput(agentBitcoinTransferLockingScript, bitcoinQuantity, false,
			false); err != nil {
			t.Fatalf("Failed to add agent bitcoin transfer output : %s", err)
		}

		// Add contract output
		instrumentTransfer.ContractIndex = uint32(len(tx.MsgTx.TxOut))
		if err := tx.AddOutput(contractLockingScript, 240, false,
			false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add action output
		transferScript, err := protocol.Serialize(transfer, true)
		if err != nil {
			t.Fatalf("Failed to serialize transfer action : %s", err)
		}

		transferScriptOutputIndex := len(tx.Outputs)
		if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add transfer action output : %s", err)
		}

		// Add funding
		fundingKey, fundingLockingScript, _ := state.MockKey()
		fundingOutpoint := state.MockOutPoint(fundingLockingScript, fundValue)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: fundingLockingScript,
			Value:         fundValue,
		})
		inputLockingScripts = append(inputLockingScripts, bitcoinLockingScript)

		if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, fundValue); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		_, changeLockingScript, _ := state.MockKey()
		tx.SetChangeLockingScript(changeLockingScript, "")

		estimate, boomerang, err := protocol.EstimatedTransferResponse(tx.MsgTx,
			inputLockingScripts, float32(feeRate), 0.0,
			[]uint64{contract1.Formation.ContractFee, 0}, true)
		if err != nil {
			t.Fatalf("Failed to estimate response size : %s", err)
		}

		t.Logf("Response estimate : %v (%d boomerang)", estimate, boomerang)

		tx.MsgTx.TxOut[instrumentTransfer.ContractIndex].Value = estimate[0]

		if _, err := tx.Sign([]bitcoin.Key{adminKey, bitcoinKey, fundingKey}); err != nil {
			t.Fatalf("Failed to sign tx : %s", err)
		}

		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

		js, _ := json.MarshalIndent(transfer, "", "  ")
		t.Logf("Transfer : %s", js)

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.Caches.Transactions.Release(ctx, transaction.GetTxID())

		agentResponseTx := broadcaster.GetLastTx()
		broadcaster.ClearTxs()
		if agentResponseTx == nil {
			t.Fatalf("No agent 1 response tx 1")
		}

		t.Logf("Agent response tx 1 : %s", agentResponseTx)

		var rejection *actions.Rejection
		for _, txout := range agentResponseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Rejection); ok {
				rejection = a
			}
		}

		if rejection == nil {
			t.Fatalf("Missing rejection action")
		}

		rejectData := actions.RejectionsData(rejection.RejectionCode)
		if rejectData != nil {
			t.Logf("Rejection Code : %s", rejectData.Label)
		}

		js, _ = json.MarshalIndent(rejection, "", "  ")
		t.Logf("Rejection : %s", js)

		if !bitcoinLockingScript.Equal(agentResponseTx.Tx.TxOut[0].LockingScript) {
			t.Fatalf("Wrong locking script for bitcoin refund output : \n  got  : %s\n  want : %s",
				agentResponseTx.Tx.TxOut[0].LockingScript, bitcoinLockingScript)
		}

		if agentResponseTx.Tx.TxOut[0].Value != bitcoinQuantity {
			t.Fatalf("Wrong value for bitcoin refund output : got  : %d, want : %d",
				agentResponseTx.Tx.TxOut[0].Value, bitcoinQuantity)
		}

		if err := bitcoin_interpreter.VerifyTx(ctx, agentResponseTx); err != nil {
			t.Fatalf("Failed to verify tx : %s", err)
		}

		time.Sleep(time.Millisecond) // wait for stats to process

		stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(contractLockingScript), uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch contract statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		if stats == nil {
			t.Fatalf("Missing contract statistics")
		}

		stats.Lock()

		statAction := stats.GetAction(actions.CodeTransfer)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != uint64(i+1) {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, i+1)
		}

		if statAction.RejectedCount != uint64(i+1) {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, i+1)
		}

		stats.Unlock()

		stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(contractLockingScript), instrument.InstrumentCode,
			uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch instrument statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		if stats == nil {
			t.Fatalf("Missing instrument statistics")
		}

		stats.Lock()

		statAction = stats.GetAction(actions.CodeTransfer)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != uint64(i+1) {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, i+1)
		}

		if statAction.RejectedCount != uint64(i+1) {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, i+1)
		}

		stats.Unlock()
	}

	agent.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript)
	StopTestAgent(ctx, t, test)
}

func Test_Transfers_Bitcoin_Approve(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	feeRate := 0.05
	broadcaster := state.NewMockTxBroadcaster()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract1, instrument := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	bitcoinKey, bitcoinLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:                contractKey,
		LockingScript:      contractLockingScript,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses, test.Statistics.Add)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	fundValue := uint64(1500)

	var receiver1Keys, receiver21Keys, receiver22Keys []bitcoin.Key
	var receiver1LockingScripts, receiver21LockingScripts, receiver22LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 10; i++ {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(instrument.InstrumentType[:]),
			InstrumentCode: instrument.InstrumentCode[:],
		}

		bitcoinTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: protocol.BSVInstrumentID,
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{
				instrumentTransfer,
				bitcoinTransfer,
			},
		}

		tx := txbuilder.NewTxBuilder(float32(feeRate), 0.0)

		var spentOutputs []*expanded_tx.Output
		var inputLockingScripts []bitcoin.Script

		// Add admin as sender
		instrumentQuantity := uint64(mathRand.Intn(1000)) + 1
		receiver1Quantities = append(receiver1Quantities, instrumentQuantity)

		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: instrumentQuantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		outpoint1 := state.MockOutPoint(adminLockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: adminLockingScript,
			Value:         1,
		})
		inputLockingScripts = append(inputLockingScripts, adminLockingScript)

		if err := tx.AddInput(*outpoint1, adminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		bitcoinQuantity1 := uint64(mathRand.Intn(1000)) + 1
		bitcoinQuantity2 := uint64(mathRand.Intn(1000)) + 1
		bitcoinQuantity := bitcoinQuantity1 + bitcoinQuantity2
		receiver2Quantities = append(receiver2Quantities, bitcoinQuantity)

		bitcoinTransfer.InstrumentSenders = append(bitcoinTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: bitcoinQuantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		outpoint2 := state.MockOutPoint(bitcoinLockingScript, bitcoinQuantity+5)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: bitcoinLockingScript,
			Value:         bitcoinQuantity + 5,
		})
		inputLockingScripts = append(inputLockingScripts, bitcoinLockingScript)

		if err := tx.AddInput(*outpoint2, bitcoinLockingScript, bitcoinQuantity+5); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add receivers
		key, lockingScript, ra := state.MockKey()
		receiver1Keys = append(receiver1Keys, key)
		receiver1LockingScripts = append(receiver1LockingScripts, lockingScript)

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: instrumentQuantity,
			})

		key, lockingScript, ra = state.MockKey()
		receiver21Keys = append(receiver21Keys, key)
		receiver21LockingScripts = append(receiver21LockingScripts, lockingScript)
		bitcoinReceiver1LockingScript := lockingScript.Copy()

		bitcoinTransfer.InstrumentReceivers = append(bitcoinTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: bitcoinQuantity1,
			})

		agentBitcoinTransferLockingScript1, err := agent_bitcoin_transfer.CreateScript(contractLockingScript,
			bitcoinReceiver1LockingScript, bitcoinLockingScript, bitcoinQuantity1,
			bitcoinLockingScript, uint32(time.Now().Unix()+100))
		if err != nil {
			t.Fatalf("Failed to create agent bitcoin transfer locking script : %s", err)
		}

		// Add agent bitcoin transfer output
		if err := tx.AddOutput(agentBitcoinTransferLockingScript1, bitcoinQuantity1, false,
			false); err != nil {
			t.Fatalf("Failed to add agent bitcoin transfer output : %s", err)
		}

		key, lockingScript, ra = state.MockKey()
		receiver22Keys = append(receiver22Keys, key)
		receiver22LockingScripts = append(receiver22LockingScripts, lockingScript)
		bitcoinReceiver2LockingScript := lockingScript.Copy()

		bitcoinTransfer.InstrumentReceivers = append(bitcoinTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: bitcoinQuantity2,
			})

		agentBitcoinTransferLockingScript2, err := agent_bitcoin_transfer.CreateScript(contractLockingScript,
			bitcoinReceiver2LockingScript, bitcoinLockingScript, bitcoinQuantity2,
			bitcoinLockingScript, uint32(time.Now().Unix()+100))
		if err != nil {
			t.Fatalf("Failed to create agent bitcoin transfer locking script : %s", err)
		}

		// Add agent bitcoin transfer output
		if err := tx.AddOutput(agentBitcoinTransferLockingScript2, bitcoinQuantity2, false,
			false); err != nil {
			t.Fatalf("Failed to add agent bitcoin transfer output : %s", err)
		}

		// Add contract output
		instrumentTransfer.ContractIndex = uint32(len(tx.MsgTx.TxOut))
		if err := tx.AddOutput(contractLockingScript, 240, false,
			false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add action output
		transferScript, err := protocol.Serialize(transfer, true)
		if err != nil {
			t.Fatalf("Failed to serialize transfer action : %s", err)
		}

		transferScriptOutputIndex := len(tx.Outputs)
		if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add transfer action output : %s", err)
		}

		// Add funding
		fundingKey, fundingLockingScript, _ := state.MockKey()
		fundingOutpoint := state.MockOutPoint(fundingLockingScript, fundValue)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: fundingLockingScript,
			Value:         fundValue,
		})
		inputLockingScripts = append(inputLockingScripts, bitcoinLockingScript)

		if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, fundValue); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		_, changeLockingScript, _ := state.MockKey()
		tx.SetChangeLockingScript(changeLockingScript, "")

		estimate, boomerang, err := protocol.EstimatedTransferResponse(tx.MsgTx,
			inputLockingScripts, float32(feeRate), 0.0,
			[]uint64{contract1.Formation.ContractFee, 0}, true)
		if err != nil {
			t.Fatalf("Failed to estimate response size : %s", err)
		}

		t.Logf("Response estimate : %v (%d boomerang)", estimate, boomerang)

		responseFeeEstimate := estimate[0] - contract1.Formation.ContractFee
		tx.MsgTx.TxOut[instrumentTransfer.ContractIndex].Value = estimate[0]

		if _, err := tx.Sign([]bitcoin.Key{adminKey, bitcoinKey, fundingKey}); err != nil {
			t.Fatalf("Failed to sign tx : %s", err)
		}

		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

		js, _ := json.MarshalIndent(transfer, "", "  ")
		t.Logf("Transfer : %s", js)

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.Caches.Transactions.Release(ctx, transaction.GetTxID())

		agentResponseTx := broadcaster.GetLastTx()
		broadcaster.ClearTxs()
		if agentResponseTx == nil {
			t.Fatalf("No agent 1 response tx 1")
		}

		t.Logf("Agent response tx 1 : %s", agentResponseTx)

		var settlement *actions.Settlement
		for _, txout := range agentResponseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Settlement); ok {
				settlement = a
			}

			if m, ok := action.(*actions.Rejection); ok {
				rejectData := actions.RejectionsData(m.RejectionCode)
				if rejectData != nil {
					t.Errorf("Rejection Code : %s", rejectData.Label)
				}

				js, _ := json.MarshalIndent(m, "", "  ")
				t.Errorf("Rejection : %s", js)
			}
		}

		if settlement == nil {
			t.Fatalf("Missing settlement action")
		}

		js, _ = json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)

		responseSize := agentResponseTx.Tx.SerializeSize()
		responseFee := fees.EstimateFeeValue(responseSize, feeRate)

		t.Logf("Response fee estimate : %d (actual %d)", responseFeeEstimate, responseFee)

		if responseFeeEstimate < responseFee {
			t.Errorf("Response fee estimate was too low : got %d, want %d", responseFeeEstimate,
				responseFee)
		}

		highResponseFeeEstimate := uint64(float64(responseFee) * 0.10)
		if highResponseFeeEstimate < 10 {
			highResponseFeeEstimate = responseFee + 10
		} else {
			highResponseFeeEstimate += responseFee
		}
		if responseFeeEstimate > highResponseFeeEstimate {
			t.Errorf("Response fee estimate was too high : got %d, want <%d", responseFeeEstimate,
				highResponseFeeEstimate)
		}

		if !bitcoinReceiver1LockingScript.Equal(agentResponseTx.Tx.TxOut[0].LockingScript) {
			t.Fatalf("Wrong locking script for bitcoin approve output : \n  got  : %s\n  want : %s",
				agentResponseTx.Tx.TxOut[0].LockingScript, bitcoinReceiver1LockingScript)
		}

		if agentResponseTx.Tx.TxOut[0].Value != bitcoinQuantity1 {
			t.Fatalf("Wrong value for bitcoin approve output : got  : %d, want : %d",
				agentResponseTx.Tx.TxOut[0].Value, bitcoinQuantity1)
		}

		if !bitcoinReceiver2LockingScript.Equal(agentResponseTx.Tx.TxOut[1].LockingScript) {
			t.Fatalf("Wrong locking script for bitcoin approve output : \n  got  : %s\n  want : %s",
				agentResponseTx.Tx.TxOut[1].LockingScript, bitcoinReceiver2LockingScript)
		}

		if agentResponseTx.Tx.TxOut[1].Value != bitcoinQuantity2 {
			t.Fatalf("Wrong value for bitcoin approve output : got  : %d, want : %d",
				agentResponseTx.Tx.TxOut[1].Value, bitcoinQuantity2)
		}

		if err := bitcoin_interpreter.VerifyTx(ctx, agentResponseTx); err != nil {
			t.Fatalf("Failed to verify tx : %s", err)
		}

		time.Sleep(time.Millisecond) // wait for stats to process

		stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(contractLockingScript), uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch contract statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		if stats == nil {
			t.Fatalf("Missing contract statistics")
		}

		stats.Lock()

		statAction := stats.GetAction(actions.CodeTransfer)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != uint64(i+1) {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, i+1)
		}

		if statAction.RejectedCount != 0 {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, 0)
		}

		stats.Unlock()

		stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(contractLockingScript), instrument.InstrumentCode,
			uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch instrument statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		if stats == nil {
			t.Fatalf("Missing instrument statistics")
		}

		stats.Lock()

		statAction = stats.GetAction(actions.CodeTransfer)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != uint64(i+1) {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, i+1)
		}

		if statAction.RejectedCount != 0 {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, 0)
		}

		stats.Unlock()
	}

	agent.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript)
	StopTestAgent(ctx, t, test)
}

func Test_AgentBitcoinTransfer_Script(t *testing.T) {
	h := "6476a9143de8668a5f8c407fa6da15b8b14955603b4e6ca688ad64768258947f75820120947f7c75206647242e8770e98fb8b5c10d2210d9d299e8672653d80312a03cbe11cac5c33c8867768258947f75820120947f7c75204b20c01aca147225835145323a36124d16452eb688e697ef988960984edad7e3886867006b6376a91436b9b22cfac90cfeaacc35ed9f2ab7d4b8cc98e188ad6c8b6b686376a914bd7ebcc998ba9fd5787ed372f43bb2cf9ff573b888ad6c8b6b686376a9145125c5c5b87083182348daa8f46389a16af4d9dc88ad6c8b6b68526ca169768254947f758254947f7c7504779566648876820128947f758254947f7c7504ffffffff87916968aa517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e76009f6301007e6840f608e7b277ed70a30a879f500cdc24ef395fab9966d07615541c27da0222143d1044147c0f5849d63e288e823215a090789e81be96c8a523e8214cfdba62d0079320e4985843644c24f6d3dca51da13ca4303f4de290393918fa3ddfa348f94aeb799521414136d08c5ed2bf3ba048afe6dcaebafeffffffffffffffffffffffffffffff007d977652795296a06394677768012080517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f517f7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e7c7e827c7e527c7e220220335fbb3f993be0a0c12b70fbc38cbea95a8c4a259e7412192534d343422cbd7a7c7e827c7e01307c7e01437e2102ba79df5f8ae7604a9830f03c7933028186aede0675a16f025dc4f8be8eec0382abac"
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatalf("Script hex : %s", err)
	}
	script := bitcoin.Script(b)
	quantity := uint64(1000)

	t.Logf("Script : %s", script)

	info, err := agent_bitcoin_transfer.MatchScript(script)
	if err != nil {
		t.Fatalf("Match : %s", err)
	}

	js, _ := json.MarshalIndent(info, "", "  ")
	t.Logf("Info : %s", js)

	t.Logf("Recover locking script : %s", info.RecoverLockingScript)

	recoverLockingScript := info.RecoverLockingScript.Copy()
	t.Logf("Refund locking script copy : %s", recoverLockingScript)
	recoverLockingScript.RemoveHardVerify()
	t.Logf("Refund locking script : %s", recoverLockingScript)

	if !info.RefundMatches(recoverLockingScript, quantity) {
		t.Errorf("Refund doesn't match")
	}
}
