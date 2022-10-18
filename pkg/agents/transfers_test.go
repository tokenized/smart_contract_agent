package agents

import (
	"bytes"
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
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
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

func Test_Transfers_Multi_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey1, contractLockingScript1, adminKey1, contract1, instrument1 := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript1, _ := state.MockKey()

	adminLockingScript1, err := adminKey1.LockingScript()
	if err != nil {
		t.Fatalf("Failed to create admin locking script : %s", err)
	}

	agent1, err := NewAgent(contractKey1, DefaultConfig(), contract1, feeLockingScript1,
		caches.Contracts, caches.Balances, caches.Transactions, caches.Subscriptions, broadcaster1)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractKey2, contractLockingScript2, adminKey2, contract2, instrument2 := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript2, _ := state.MockKey()

	adminLockingScript2, err := adminKey2.LockingScript()
	if err != nil {
		t.Fatalf("Failed to create admin locking script : %s", err)
	}

	agent2, err := NewAgent(contractKey2, DefaultConfig(), contract2, feeLockingScript2,
		caches.Contracts, caches.Balances, caches.Transactions, caches.Subscriptions, broadcaster2)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 100; i++ {
		instrumentTransfer1 := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(instrument1.InstrumentType[:]),
			InstrumentCode: instrument1.InstrumentCode[:],
		}

		instrumentTransfer2 := &actions.InstrumentTransferField{
			ContractIndex:  1,
			InstrumentType: string(instrument2.InstrumentType[:]),
			InstrumentCode: instrument2.InstrumentCode[:],
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{
				instrumentTransfer1,
				instrumentTransfer2,
			},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		var spentOutputs []*expanded_tx.Output

		// Add admin as sender
		quantity1 := uint64(mathRand.Intn(1000)) + 1
		receiver1Quantities = append(receiver1Quantities, quantity1)

		instrumentTransfer1.InstrumentSenders = append(instrumentTransfer1.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity1,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		outpoint1 := state.MockOutPoint(adminLockingScript1, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: adminLockingScript1,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint1, adminLockingScript1, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		quantity2 := uint64(mathRand.Intn(1000)) + 1
		receiver2Quantities = append(receiver2Quantities, quantity2)

		instrumentTransfer2.InstrumentSenders = append(instrumentTransfer2.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity2,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		outpoint2 := state.MockOutPoint(adminLockingScript2, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: adminLockingScript2,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint2, adminLockingScript2, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add receivers
		key, lockingScript, ra := state.MockKey()
		receiver1Keys = append(receiver1Keys, key)
		receiver1LockingScripts = append(receiver1LockingScripts, lockingScript)

		instrumentTransfer1.InstrumentReceivers = append(instrumentTransfer1.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity1,
			})

		key, lockingScript, ra = state.MockKey()
		receiver2Keys = append(receiver2Keys, key)
		receiver2LockingScripts = append(receiver2LockingScripts, lockingScript)

		instrumentTransfer2.InstrumentReceivers = append(instrumentTransfer2.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity2,
			})

		// Add contract outputs
		if err := tx.AddOutput(contractLockingScript1, 240, false, false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}
		if err := tx.AddOutput(contractLockingScript2, 200, false, false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add boomerang output
		if err := tx.AddOutput(contractLockingScript1, 200, false, false); err != nil {
			t.Fatalf("Failed to add boomerang output : %s", err)
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
		fundingOutpoint := state.MockOutPoint(fundingLockingScript, 1000)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: fundingLockingScript,
			Value:         1000,
		})

		if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 1000); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		_, changeLockingScript, _ := state.MockKey()
		tx.SetChangeLockingScript(changeLockingScript, "")

		if _, err := tx.Sign([]bitcoin.Key{adminKey1, adminKey2, fundingKey}); err != nil {
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
		if err := agent1.Process(ctx, transaction, []actions.Action{transfer}, now); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		agent1ResponseTx := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx == nil {
			t.Fatalf("No agent 1 response tx 1")
		}

		t.Logf("Agent 1 response tx 1 : %s", agent1ResponseTx)

		// Find settlement request action
		var message *actions.Message
		for _, txout := range agent1ResponseTx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				message = a
			}
		}

		if message == nil {
			t.Fatalf("Missing message action")
		}

		if message.MessageCode != messages.CodeSettlementRequest {
			t.Fatalf("Wrong response message code : got %d, want %d", message.MessageCode,
				messages.CodeSettlementRequest)
		} else {
			t.Logf("Response 1 message is settlement request")
		}

		js, _ := json.MarshalIndent(message, "", "  ")
		t.Logf("Message : %s", js)

		if err := agent2.Process(ctx, transaction, []actions.Action{transfer}, now); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		caches.Transactions.Release(ctx, transaction.GetTxID())

		agent2ResponseTx := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx != nil {
			t.Fatalf("Should be no response tx 1 from agent 2 : %s", agent2ResponseTx)
		}

		t.Logf("Agent 2 no response tx 1")

		messageSpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: tx.MsgTx.TxOut[2].LockingScript,
				Value:         tx.MsgTx.TxOut[2].Value,
			},
		}

		addMessageTransaction := &state.Transaction{
			Tx:           agent1ResponseTx,
			SpentOutputs: messageSpentOutputs,
		}

		messageTransaction, err := caches.Transactions.Add(ctx, addMessageTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, messageTransaction, []actions.Action{message},
			now); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent1ResponseTx2 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx2 != nil {
			t.Fatalf("Agent 1 response tx 2 should be nil : %s", agent1ResponseTx2)
		}

		if err := agent2.Process(ctx, messageTransaction, []actions.Action{message},
			now); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent2ResponseTx2 := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx2 == nil {
			t.Fatalf("No agent 2 response tx 2")
		}

		t.Logf("Agent 2 response tx 2 : %s", agent2ResponseTx2)

		// Find signature request action
		var message2 *actions.Message
		for _, txout := range agent2ResponseTx2.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				message2 = a
			}
		}

		if message2 == nil {
			t.Fatalf("Missing message action")
		}

		if message2.MessageCode != messages.CodeSignatureRequest {
			t.Fatalf("Wrong response message code : got %d, want %d", message2.MessageCode,
				messages.CodeSignatureRequest)
		} else {
			t.Logf("Response 2 message is signature request")
		}

		t.Logf("Message payload : %x", message2.MessagePayload)

		messagePayload, err := messages.Deserialize(message2.MessageCode, message2.MessagePayload)
		if err != nil {
			t.Fatalf("Failed to deserialize message payload : %s", err)
		}

		sigRequestPayload, ok := messagePayload.(*messages.SignatureRequest)
		if !ok {
			t.Fatalf("Message payload not a sig request")
		}

		sigRequestTx := &wire.MsgTx{}
		if err := sigRequestTx.Deserialize(bytes.NewReader(sigRequestPayload.Payload)); err != nil {
			t.Fatalf("Failed to decode sig request tx : %s", err)
		}

		t.Logf("Sig request tx : %s", sigRequestTx)

		var sigSettlement *actions.Settlement
		for _, txout := range sigRequestTx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Settlement); ok {
				sigSettlement = a
			}
		}

		if sigSettlement == nil {
			t.Fatalf("Missing settlement in sig request")
		}

		caches.Transactions.Release(ctx, messageTransaction.GetTxID())

		message2SpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: agent1ResponseTx.TxOut[0].LockingScript,
				Value:         agent1ResponseTx.TxOut[0].Value,
			},
		}

		addMessage2Transaction := &state.Transaction{
			Tx:           agent2ResponseTx2,
			SpentOutputs: message2SpentOutputs,
		}

		message2Transaction, err := caches.Transactions.Add(ctx, addMessage2Transaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent2.Process(ctx, message2Transaction, []actions.Action{message2},
			now); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent2ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx3 != nil {
			t.Fatalf("Agent 2 response tx 3 should be nil : %s", agent2ResponseTx3)
		}

		if err := agent1.Process(ctx, message2Transaction, []actions.Action{message2},
			now); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent1ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx3 == nil {
			t.Fatalf("No agent 1 response tx 3")
		}

		t.Logf("Agent 1 response tx 3 : %s", agent1ResponseTx3)

		var settlement *actions.Settlement
		for _, txout := range agent1ResponseTx3.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Settlement); ok {
				settlement = a
			}

			if m, ok := action.(*actions.Rejection); ok {
				js, _ = json.MarshalIndent(m, "", "  ")
				t.Logf("Rejection : %s", js)
			}
		}

		if settlement == nil {
			t.Fatalf("Missing settlement action")
		}

		js, _ = json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)

		caches.Transactions.Release(ctx, message2Transaction.GetTxID())
	}

	caches.Contracts.Release(ctx, contractLockingScript1)
	caches.Contracts.Release(ctx, contractLockingScript2)
	caches.StopTestCaches()
}
