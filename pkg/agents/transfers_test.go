package agents

import (
	"context"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/json"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Transfers_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, contract, instrument := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	adminLockingScript, err := adminKey.LockingScript()
	if err != nil {
		t.Fatalf("Failed to create admin locking script : %s", err)
	}

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript,
		caches.Contracts, caches.Balances, caches.Transactions, caches.Subscriptions, broadcaster)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	var receiverKeys []bitcoin.Key
	var receiverLockingScripts []bitcoin.Script
	var receiverQuantities []uint64
	for i := 0; i < 100; i++ {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(instrument.InstrumentType[:]),
			InstrumentCode: instrument.InstrumentCode[:],
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		var spentOutputs []*expanded_tx.Output

		// Add admin as sender
		quantity := uint64(mathRand.Intn(1000))
		receiverQuantities = append(receiverQuantities, quantity)

		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		outpoint := state.MockOutPoint(adminLockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: adminLockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add receivers
		key, lockingScript, ra := state.MockKey()
		receiverKeys = append(receiverKeys, key)
		receiverLockingScripts = append(receiverLockingScripts, lockingScript)

		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		// Add contract output
		if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add action output
		transferScript, err := protocol.Serialize(transfer, true)
		if err != nil {
			t.Fatalf("Failed to serialize transfer action : %s", err)
		}

		if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add transfer action output : %s", err)
		}

		// Add funding
		fundingKey, fundingLockingScript, _ := state.MockKey()
		fundingOutpoint := state.MockOutPoint(fundingLockingScript, 300)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: fundingLockingScript,
			Value:         300,
		})

		if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 300); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		_, changeLockingScript, _ := state.MockKey()
		tx.SetChangeLockingScript(changeLockingScript, "")

		if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
			t.Fatalf("Failed to sign tx : %s", err)
		}

		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

		addTransaction := &state.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		now := uint64(time.Now().UnixNano())
		if err := agent.Process(ctx, transaction, []actions.Action{transfer}, now); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		responseTx := broadcaster.GetLastTx()
		if responseTx == nil {
			t.Fatalf("No response tx")
		}

		t.Logf("Response Tx : %s", responseTx)

		// Find settlement action
		var settlement *actions.Settlement
		for _, txout := range responseTx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			s, ok := action.(*actions.Settlement)
			if ok {
				settlement = s
			}
		}

		if settlement == nil {
			t.Fatalf("Missing settlement action")
		}

		js, _ := json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)

		caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	receiverOffset := 0
	var finalLockingScripts []bitcoin.Script
	var finalQuantities []uint64
	for {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(instrument.InstrumentType[:]),
			InstrumentCode: instrument.InstrumentCode[:],
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		var keys []bitcoin.Key
		var spentOutputs []*expanded_tx.Output

		// Add senders
		senderCount := mathRand.Intn(5) + 1
		if receiverOffset+senderCount >= len(receiverKeys) {
			break
		}

		senderQuantity := uint64(0)
		for s := 0; s < senderCount; s++ {
			lockingScript := receiverLockingScripts[receiverOffset]
			quantity := receiverQuantities[receiverOffset]
			keys = append(keys, receiverKeys[receiverOffset])
			receiverOffset++
			senderQuantity += quantity

			// Add sender
			instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
				&actions.QuantityIndexField{
					Quantity: quantity,
					Index:    uint32(len(tx.MsgTx.TxIn)),
				})

			// Add input
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
			quantity := uint64(mathRand.Intn(1000))
			if quantity > senderQuantity {
				quantity = senderQuantity
			}
			finalQuantities = append(finalQuantities, quantity)

			_, lockingScript, ra := state.MockKey()
			finalLockingScripts = append(finalLockingScripts, lockingScript)
			instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
				&actions.InstrumentReceiverField{
					Address:  ra.Bytes(),
					Quantity: quantity,
				})

			senderQuantity -= quantity
			if senderQuantity == 0 {
				break
			}
		}

		// Add contract output
		if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add action output
		transferScript, err := protocol.Serialize(transfer, true)
		if err != nil {
			t.Fatalf("Failed to serialize transfer action : %s", err)
		}

		if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add transfer action output : %s", err)
		}

		// Add funding
		key, lockingScript, _ := state.MockKey()
		keys = append(keys, key)
		outpoint := state.MockOutPoint(lockingScript, 300)
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

		addTransaction := &state.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		now := uint64(time.Now().UnixNano())
		if err := agent.Process(ctx, transaction, []actions.Action{transfer}, now); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		responseTx := broadcaster.GetLastTx()
		if responseTx == nil {
			t.Fatalf("No response tx")
		}

		t.Logf("Response Tx : %s", responseTx)

		// Find settlement action
		var settlement *actions.Settlement
		for _, txout := range responseTx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			s, ok := action.(*actions.Settlement)
			if ok {
				settlement = s
			}
		}

		if settlement == nil {
			t.Fatalf("Missing settlement action")
		}

		js, _ := json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)

		caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	// Check balances
	for i, lockingScript := range finalLockingScripts {
		balance, err := caches.Balances.Get(ctx, contractLockingScript, instrument.InstrumentCode,
			lockingScript)
		if err != nil {
			t.Fatalf("Failed to get final balance : %s", err)
		}

		if balance == nil {
			t.Fatalf("Missing final balance : %s", lockingScript)
		}

		balance.Lock()
		if balance.Quantity != finalQuantities[i] {
			t.Errorf("Wrong final balance : got %d, want %d : %s", balance.Quantity,
				finalQuantities[i], lockingScript)
		} else {
			t.Logf("Verified balance : %d : %s", balance.Quantity, lockingScript)
		}

		if len(balance.Adjustments) != 0 {
			t.Errorf("Remaining adjustements : %d : %s", len(balance.Adjustments), lockingScript)
		}
		balance.Unlock()

		caches.Balances.Release(ctx, contractLockingScript, instrument.InstrumentCode, balance)
	}

	caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

// Test_Transfers_InsufficientQuantity creates a transfer action for locking scripts that don't have
// any tokens and will be rejected for insufficient quantity.
func Test_Transfers_InsufficientQuantity(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, _, contract, instrument := state.MockInstrument(ctx, caches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript,
		caches.Contracts, caches.Balances, caches.Transactions, caches.Subscriptions, broadcaster)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(instrument.InstrumentType[:]),
		InstrumentCode: instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	var keys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output

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
	if err := tx.AddOutput(contractLockingScript, 100, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(transfer, "", "  ")
	t.Logf("Transfer : %s", js)

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	key, lockingScript, _ := state.MockKey()
	keys = append(keys, key)
	outpoint := state.MockOutPoint(lockingScript, 200)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{transfer}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find rejection action
	var rejection *actions.Rejection
	for _, txout := range responseTx.TxOut {
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
