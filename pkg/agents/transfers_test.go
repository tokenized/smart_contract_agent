package agents

import (
	"bytes"
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
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/json"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/threads"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"

	"github.com/pkg/errors"
)

func Test_Transfers_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	var receiverKeys []bitcoin.Key
	var receiverLockingScripts []bitcoin.Script
	var receiverQuantities []uint64
	for i := 0; i < 100; i++ {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(test.instrument.InstrumentType[:]),
			InstrumentCode: test.instrument.InstrumentCode[:],
		}

		transfer := &actions.Transfer{
			Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		var spentOutputs []*expanded_tx.Output

		// Add admin as sender
		quantity := uint64(mathRand.Intn(1000)) + 1
		receiverQuantities = append(receiverQuantities, quantity)

		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})

		// Add input
		outpoint := state.MockOutPoint(test.adminLockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: test.adminLockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
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
		if err := tx.AddOutput(test.contractLockingScript, 200, false, false); err != nil {
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

		if _, err := tx.Sign([]bitcoin.Key{test.adminKey, fundingKey}); err != nil {
			t.Fatalf("Failed to sign tx : %s", err)
		}

		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: test.contractLockingScript,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		responseTx := test.broadcaster.GetLastTx()
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

		test.caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	receiverOffset := 0
	var finalLockingScripts []bitcoin.Script
	var finalQuantities []uint64
	for {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(test.instrument.InstrumentType[:]),
			InstrumentCode: test.instrument.InstrumentCode[:],
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
			quantity := uint64(mathRand.Intn(1000)) + 1
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
		if err := tx.AddOutput(test.contractLockingScript, 200, false, false); err != nil {
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

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: test.contractLockingScript,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		responseTx := test.broadcaster.GetLastTx()
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

		test.caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	// Check balances
	for i, lockingScript := range finalLockingScripts {
		balance, err := test.caches.Caches.Balances.Get(ctx, test.contractLockingScript,
			test.instrument.InstrumentCode, lockingScript)
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

		test.caches.Caches.Balances.Release(ctx, test.contractLockingScript,
			test.instrument.InstrumentCode, balance)
	}

	StopTestAgent(ctx, t, test)
}

// Test_Transfers_InsufficientQuantity creates a transfer action for locking scripts that don't have
// any tokens and will be rejected for insufficient quantity.
func Test_Transfers_InsufficientQuantity(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.instrument.InstrumentType[:]),
		InstrumentCode: test.instrument.InstrumentCode[:],
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
	mockBalances := state.MockBalances(ctx, &test.caches.TestCaches, test.contract,
		test.instrument, senderCount)
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
		keys = append(keys, mockBalance.Key)
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
	for {
		_, lockingScript, ra := state.MockKey()
		lockingScripts = append(lockingScripts, lockingScript)
		resultQuantities = append(resultQuantities, 0)
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
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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

	transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	balances, err := test.caches.TestCaches.Caches.Balances.GetMulti(ctx, test.contractLockingScript,
		test.instrument.InstrumentCode, lockingScripts)
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

	if err := test.caches.TestCaches.Caches.Balances.ReleaseMulti(ctx, test.contractLockingScript,
		test.instrument.InstrumentCode, balances); err != nil {
		t.Fatalf("Failed to release balances : %s", err)
	}

	StopTestAgent(ctx, t, test)

	responseTx := test.broadcaster.GetLastTx()
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

// Test_Transfers_NoQuantity creates a transfer action for locking scripts that don't have
// any tokens and will be rejected for insufficient quantity.
func Test_Transfers_NoQuantity(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.instrument.InstrumentType[:]),
		InstrumentCode: test.instrument.InstrumentCode[:],
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
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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

	transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	balances, err := test.caches.TestCaches.Caches.Balances.GetMulti(ctx, test.contractLockingScript,
		test.instrument.InstrumentCode, lockingScripts)
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

	if err := test.caches.TestCaches.Caches.Balances.ReleaseMulti(ctx, test.contractLockingScript,
		test.instrument.InstrumentCode, balances); err != nil {
		t.Fatalf("Failed to release balances : %s", err)
	}

	StopTestAgent(ctx, t, test)

	responseTx := test.broadcaster.GetLastTx()
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
		InstrumentType: string(test.instrument.InstrumentType[:]),
		InstrumentCode: test.instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.adminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
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
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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

	transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	StopTestAgent(ctx, t, test)

	responseTx := test.broadcaster.GetLastTx()
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

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to create contract address : %s", err)
	}

	headerHeight := 100045
	var headerHash bitcoin.Hash32
	rand.Read(headerHash[:])
	test.headers.AddHash(headerHeight, headerHash)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.instrument.InstrumentType[:]),
		InstrumentCode: test.instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.adminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1000
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	for {
		_, _, receiverAddress := state.MockKey()

		sigHash, err := protocol.TransferOracleSigHash(ctx, contractAddress,
			test.instrument.InstrumentCode[:], receiverAddress, headerHash, 0, 1)
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
	if err := tx.AddOutput(test.contractLockingScript, 200, false, false); err != nil {
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

	transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	StopTestAgent(ctx, t, test)

	responseTx := test.broadcaster.GetLastTx()
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

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to create contract address : %s", err)
	}

	headerHeight := 100045
	var headerHash bitcoin.Hash32
	rand.Read(headerHash[:])
	test.headers.AddHash(headerHeight, headerHash)

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.instrument.InstrumentType[:]),
		InstrumentCode: test.instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	keys := []bitcoin.Key{test.adminKey}
	var spentOutputs []*expanded_tx.Output

	// Add senders
	senderQuantity := uint64(mathRand.Intn(5000)) + 1000
	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: senderQuantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: test.adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	for {
		_, _, receiverAddress := state.MockKey()

		sigHash, err := protocol.TransferOracleSigHash(ctx, contractAddress,
			test.instrument.InstrumentCode[:], receiverAddress, headerHash, 0, 1)
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
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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

	transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	StopTestAgent(ctx, t, test)

	responseTx := test.broadcaster.GetLastTx()
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

func Test_Transfers_Multi_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster1, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster2, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 1; i++ {
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

		transferScriptOutputIndex := len(tx.Outputs)
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
		transferTxID := tx.TxID()

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
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
		messageScriptOutputIndex := 0
		for outputIndex, txout := range agent1ResponseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				messageScriptOutputIndex = outputIndex
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

		if err := agent2.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

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

		addMessageTransaction := &transactions.Transaction{
			Tx:           agent1ResponseTx.Tx,
			SpentOutputs: messageSpentOutputs,
		}

		messageTransaction, err := test.caches.Transactions.Add(ctx, addMessageTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent1ResponseTx2 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx2 != nil {
			t.Fatalf("Agent 1 response tx 2 should be nil : %s", agent1ResponseTx2)
		}

		if err := agent2.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
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
		message2ScriptOutputIndex := 0
		for outputIndex, txout := range agent2ResponseTx2.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				message2ScriptOutputIndex = outputIndex
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

		test.caches.Transactions.Release(ctx, messageTransaction.GetTxID())

		// Check that the balances do have pending adjustement.
		balance, err := test.caches.Caches.Balances.Get(ctx, contractLockingScript1,
			instrument1.InstrumentCode, adminLockingScript1)
		if err != nil {
			t.Fatalf("Failed to get admin balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance before signature request : %s", js)

		foundAdjustments := 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()
		test.caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments == 0 {
			t.Fatalf("Pending adjustment not found before signature request")
		} else if foundAdjustments != 1 {
			t.Fatalf("Wrong number of pending adjustments found before signature request : got %d, want %d",
				foundAdjustments, 1)
		} else {
			t.Logf("Found pending adjustment before signature request")
		}

		// Check that the balances do have pending adjustement.
		balance, err = test.caches.Caches.Balances.Get(ctx, contractLockingScript1,
			instrument1.InstrumentCode, receiver1LockingScripts[0])
		if err != nil {
			t.Fatalf("Failed to get receiver balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance before signature request : %s", js)

		foundAdjustments = 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()
		test.caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments == 0 {
			t.Fatalf("Pending adjustment not found before signature request")
		} else if foundAdjustments != 1 {
			t.Fatalf("Wrong number of pending adjustments found before signature request : got %d, want %d",
				foundAdjustments, 1)
		} else {
			t.Logf("Found pending adjustment before signature request")
		}

		// Check that the balances do have pending adjustement.
		balance, err = test.caches.Caches.Balances.Get(ctx, contractLockingScript2,
			instrument2.InstrumentCode, receiver2LockingScripts[0])
		if err != nil {
			t.Fatalf("Failed to get receiver balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance before signature request : %s", js)

		foundAdjustments = 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()
		test.caches.Caches.Balances.Release(ctx, contractLockingScript2,
			instrument2.InstrumentCode, balance)

		if foundAdjustments == 0 {
			t.Fatalf("Pending adjustment not found before signature request")
		} else if foundAdjustments != 1 {
			t.Fatalf("Wrong number of pending adjustments found before signature request : got %d, want %d",
				foundAdjustments, 1)
		} else {
			t.Logf("Found pending adjustment before signature request")
		}

		message2SpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: agent1ResponseTx.Tx.TxOut[0].LockingScript,
				Value:         agent1ResponseTx.Tx.TxOut[0].Value,
			},
		}

		addMessage2Transaction := &transactions.Transaction{
			Tx:           agent2ResponseTx2.Tx,
			SpentOutputs: message2SpentOutputs,
		}

		message2Transaction, err := test.caches.Transactions.Add(ctx, addMessage2Transaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent2.Process(ctx, message2Transaction, []Action{{
			OutputIndex: message2ScriptOutputIndex,
			Action:      message2,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent2ResponseTx3 := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx3 != nil {
			t.Fatalf("Agent 2 response tx 3 should be nil : %s", agent2ResponseTx3)
		}

		if err := agent1.Process(ctx, message2Transaction, []Action{{
			OutputIndex: message2ScriptOutputIndex,
			Action:      message2,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent1ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx3 == nil {
			t.Fatalf("No agent 1 response tx 3")
		}

		t.Logf("Agent 1 response tx 3 : %s", agent1ResponseTx3)

		var settlement *actions.Settlement
		settlementScriptOutputIndex := 0
		for outputIndex, txout := range agent1ResponseTx3.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Settlement); ok {
				settlementScriptOutputIndex = outputIndex
				settlement = a
			}

			if m, ok := action.(*actions.Rejection); ok {
				rejectData := actions.RejectionsData(m.RejectionCode)
				if rejectData != nil {
					t.Logf("Rejection Code : %s", rejectData.Label)
				}

				js, _ = json.MarshalIndent(m, "", "  ")
				t.Logf("Rejection : %s", js)
			}
		}

		if settlement == nil {
			t.Fatalf("Missing settlement action")
		}

		js, _ = json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)

		test.caches.Transactions.Release(ctx, message2Transaction.GetTxID())

		// Check that the balances don't have any adjustements pending.
		balance, err = test.caches.Caches.Balances.Get(ctx, contractLockingScript1,
			instrument1.InstrumentCode, adminLockingScript1)
		if err != nil {
			t.Fatalf("Failed to get admin balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance : %s", js)

		foundAdjustments = 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()

		test.caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}

		// Check that the balances don't have any adjustements pending.
		balance, err = test.caches.Caches.Balances.Get(ctx, contractLockingScript1,
			instrument1.InstrumentCode, receiver1LockingScripts[0])
		if err != nil {
			t.Fatalf("Failed to get receiver balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance : %s", js)

		foundAdjustments = 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()

		test.caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}

		settlementSpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: tx.MsgTx.TxOut[0].LockingScript,
				Value:         tx.MsgTx.TxOut[0].Value,
			},
			{
				LockingScript: tx.MsgTx.TxOut[1].LockingScript,
				Value:         tx.MsgTx.TxOut[1].Value,
			},
		}

		addSettlementTransaction := &transactions.Transaction{
			Tx:           agent1ResponseTx3.Tx,
			SpentOutputs: settlementSpentOutputs,
		}

		settlementTransaction, err := test.caches.Transactions.Add(ctx, addSettlementTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent2.Process(ctx, settlementTransaction, []Action{{
			OutputIndex: settlementScriptOutputIndex,
			Action:      settlement,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     false,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent2ResponseTx4 := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx4 != nil {
			t.Fatalf("Agent 2 response tx 4 should be nil : %s", agent2ResponseTx4)
		}

		// Check that the balances don't have any adjustements pending.
		balance, err = test.caches.Caches.Balances.Get(ctx, contractLockingScript2,
			instrument2.InstrumentCode, receiver2LockingScripts[0])
		if err != nil {
			t.Fatalf("Failed to get receiver balance : %s", err)
		}

		balance.Lock()
		js, _ = json.MarshalIndent(balance, "", "  ")
		balance.Unlock()
		t.Logf("Balance : %s", js)

		foundAdjustments = 0
		balance.Lock()
		for _, adj := range balance.Adjustments {
			if adj.TxID == nil {
				continue
			}

			if adj.TxID.Equal(&transferTxID) {
				foundAdjustments++
			}
		}
		balance.Unlock()

		test.caches.Caches.Balances.Release(ctx, contractLockingScript2,
			instrument2.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

func Test_Transfers_Bitcoin_Refund(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	feeRate := 0.05
	broadcaster := state.NewMockTxBroadcaster()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract1, instrument := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	bitcoinKey, bitcoinLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:                contractKey,
		LockingScript:      contractLockingScript,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
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

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
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

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

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
	}

	agent.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript)
	StopTestAgent(ctx, t, test)
}

func Test_Transfers_Bitcoin_Approve(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	feeRate := 0.05
	broadcaster := state.NewMockTxBroadcaster()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract1, instrument := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	bitcoinKey, bitcoinLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:                contractKey,
		LockingScript:      contractLockingScript,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
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

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
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

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

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
	}

	agent.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript)
	StopTestAgent(ctx, t, test)
}

func Test_Transfers_Multi_Expire(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	config := DefaultConfig()
	config.MultiContractExpiration.Duration = time.Millisecond * 250

	scheduler := scheduler.NewScheduler(broadcaster1, time.Second)
	_, feeLockingScript, _ := state.MockKey()

	schedulerInterrupt := make(chan interface{})
	go func() {
		defer func() {
			if err := recover(); err != nil {
				t.Errorf("Scheduler panic : %s", err)
			}
		}()

		if err := scheduler.Run(ctx, schedulerInterrupt); err != nil &&
			errors.Cause(err) != threads.Interrupted {
			t.Errorf("Scheduler returned an error : %s", err)
		}
	}()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.caches.TestCaches)

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, config, test.caches.Caches, test.caches.Transactions,
		test.caches.Services, test.locker, test.store, broadcaster1, nil, nil, scheduler,
		test.mockStore, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	test.mockStore.Add(agentData1)

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.caches.TestCaches)

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, config, test.caches.Caches, test.caches.Transactions,
		test.caches.Services, test.locker, test.store, broadcaster2, nil, nil, scheduler,
		test.mockStore, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	test.mockStore.Add(agentData2)

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 1; i++ {
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

		transferScriptOutputIndex := len(tx.Outputs)
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

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
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
		messageScriptOutputIndex := 0
		for outputIndex, txout := range agent1ResponseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				messageScriptOutputIndex = outputIndex
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

		if err := agent2.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

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

		addMessageTransaction := &transactions.Transaction{
			Tx:           agent1ResponseTx.Tx,
			SpentOutputs: messageSpentOutputs,
		}

		messageTransaction, err := test.caches.Transactions.Add(ctx, addMessageTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		t.Logf("Sleeping to wait for expiration")
		time.Sleep(time.Millisecond * 300)

		agent1ResponseTxReject := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTxReject == nil {
			t.Fatalf("No agent 1 response tx reject")
		}

		t.Logf("Agent 1 response tx reject : %s", agent1ResponseTxReject)

		// Find signature request action
		var messageReject *actions.Rejection
		for _, txout := range agent1ResponseTxReject.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Rejection); ok {
				messageReject = a
			}
		}

		if messageReject == nil {
			t.Fatalf("Missing message reject action")
		}

		if messageReject.RejectionCode != actions.RejectionsTransferExpired {
			t.Fatalf("Wrong response message rejection code : got %d, want %d",
				messageReject.RejectionCode, actions.RejectionsTransferExpired)
		} else {
			t.Logf("Response message is transfer expired rejection")
		}

		js, _ = json.MarshalIndent(messageReject, "", "  ")
		t.Logf("Rejection Message : %s", js)

		t.Logf("Processing settlement request")
		if err := agent1.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent1ResponseTx2 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx2 != nil {
			t.Fatalf("Agent 1 response tx 2 should be nil : %s", agent1ResponseTx2)
		}

		if err := agent2.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
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
		message2ScriptOutputIndex := 0
		for outputIndex, txout := range agent2ResponseTx2.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				message2ScriptOutputIndex = outputIndex
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

		js, _ = json.MarshalIndent(message2, "", "  ")
		t.Logf("Signature Request Message : %s", js)

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

		test.caches.Transactions.Release(ctx, messageTransaction.GetTxID())

		message2SpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: agent1ResponseTx.Tx.TxOut[0].LockingScript,
				Value:         agent1ResponseTx.Tx.TxOut[0].Value,
			},
		}

		addMessage2Transaction := &transactions.Transaction{
			Tx:           agent2ResponseTx2.Tx,
			SpentOutputs: message2SpentOutputs,
		}

		message2Transaction, err := test.caches.Transactions.Add(ctx, addMessage2Transaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		t.Logf("Processing signature request")
		if err := agent2.Process(ctx, message2Transaction, []Action{{
			OutputIndex: message2ScriptOutputIndex,
			Action:      message2,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent2ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx3 != nil {
			t.Fatalf("Agent 2 response tx 3 should be nil : %s", agent2ResponseTx3)
		}

		if err := agent1.Process(ctx, message2Transaction, []Action{{
			OutputIndex: message2ScriptOutputIndex,
			Action:      message2,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message 2 transaction : %s", err)
		}

		agent1ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx3 != nil {
			t.Fatalf("Agent 1 response tx 3 should be nil")
		}

		test.caches.Transactions.Release(ctx, message2Transaction.GetTxID())
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

// Test_Transfers_Multi_Reject_First is a multi-contract transfer that is rejected by the first
// contract agent.
func Test_Transfers_Multi_Reject_First(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster1, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster2, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 1; i++ {
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
				Quantity: quantity1 + 1,
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

		transferScriptOutputIndex := len(tx.Outputs)
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

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		agent1ResponseTx := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx == nil {
			t.Fatalf("No agent 1 response tx 1")
		}

		t.Logf("Agent 1 response tx 1 : %s", agent1ResponseTx)

		// Find rejection action
		var rejection *actions.Rejection
		for _, txout := range agent1ResponseTx.Tx.TxOut {
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

		if rejection.RejectionCode != actions.RejectionsMsgMalformed {
			t.Fatalf("Wrong response rejection code : got %d, want %d", rejection.RejectionCode,
				actions.RejectionsMsgMalformed)
		}

		js, _ := json.MarshalIndent(rejection, "", "  ")
		t.Logf("Rejection : %s", js)

		if err := agent2.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

		agent2ResponseTx := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx != nil {
			t.Fatalf("Should be no response tx 1 from agent 2 : %s", agent2ResponseTx)
		}

		t.Logf("Agent 2 no response tx 1")
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

// Test_Transfers_Multi_Reject_Second is a multi-contract transfer that is rejected by the second
// contract agent.
func Test_Transfers_Multi_Reject_Second(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := StartTestData(ctx, t)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster1, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.caches.Caches,
		test.caches.Transactions, test.caches.Services, test.locker, test.store, broadcaster2, nil,
		nil, nil, nil, test.peerChannelsFactory, test.peerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 1; i++ {
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
				Quantity: quantity2 + 1,
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

		transferScriptOutputIndex := len(tx.Outputs)
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

		addTransaction := &transactions.Transaction{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}
		transferTxID := tx.MsgTx.TxHash()

		transaction, err := test.caches.Transactions.Add(ctx, addTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
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
		messageScriptOutputIndex := 0
		for outputIndex, txout := range agent1ResponseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Message); ok {
				messageScriptOutputIndex = outputIndex
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

		if err := agent2.Process(ctx, transaction, []Action{{
			OutputIndex: transferScriptOutputIndex,
			Action:      transfer,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transfer transaction : %s", err)
		}

		test.caches.Transactions.Release(ctx, transaction.GetTxID())

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

		addMessageTransaction := &transactions.Transaction{
			Tx:           agent1ResponseTx.Tx,
			SpentOutputs: messageSpentOutputs,
		}

		messageTransaction, err := test.caches.Transactions.Add(ctx, addMessageTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent1ResponseTx2 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx2 != nil {
			t.Fatalf("Agent 1 response tx 2 should be nil : %s", agent1ResponseTx2)
		}

		if err := agent2.Process(ctx, messageTransaction, []Action{{
			OutputIndex: messageScriptOutputIndex,
			Action:      message,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript2,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process message transaction : %s", err)
		}

		agent2ResponseTx2 := broadcaster2.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2ResponseTx2 == nil {
			t.Fatalf("No agent 2 response tx 2")
		}

		t.Logf("Agent 2 response tx 2 : %s", agent2ResponseTx2)

		// Find rejection action
		var rejection *actions.Rejection
		rejectionScriptOutputIndex := 0
		for outputIndex, txout := range agent2ResponseTx2.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.Rejection); ok {
				rejectionScriptOutputIndex = outputIndex
				rejection = a
			}
		}

		if rejection == nil {
			t.Fatalf("Missing rejection action")
		}

		if rejection.RejectionCode != actions.RejectionsMsgMalformed {
			t.Fatalf("Wrong response rejection code : got %d, want %d", rejection.RejectionCode,
				actions.RejectionsMsgMalformed)
		}

		js, _ = json.MarshalIndent(rejection, "", "  ")
		t.Logf("Rejection : %s", js)

		test.caches.Transactions.Release(ctx, messageTransaction.GetTxID())

		rejectionSpentOutputs := []*expanded_tx.Output{
			{
				LockingScript: agent1ResponseTx.Tx.TxOut[0].LockingScript,
				Value:         agent1ResponseTx.Tx.TxOut[0].Value,
			},
		}

		addRejectionTransaction := &transactions.Transaction{
			Tx:           agent2ResponseTx2.Tx,
			SpentOutputs: rejectionSpentOutputs,
		}

		rejectionTransaction, err := test.caches.Transactions.Add(ctx, addRejectionTransaction)
		if err != nil {
			t.Fatalf("Failed to add transaction : %s", err)
		}

		if err := agent1.Process(ctx, rejectionTransaction, []Action{{
			OutputIndex: rejectionScriptOutputIndex,
			Action:      rejection,
			Agents: []ActionAgent{
				{
					LockingScript: contractLockingScript1,
					IsRequest:     false,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process rejection transaction : %s", err)
		}

		agent1ResponseTx3 := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1ResponseTx3 == nil {
			t.Fatalf("Agent 1 response tx 3 should not be nil")
		}

		t.Logf("Agent 2 response tx 3 : %s", agent1ResponseTx3)

		// Find rejection action
		rejection = nil
		for _, txout := range agent1ResponseTx3.Tx.TxOut {
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

		if rejection.RejectionCode != actions.RejectionsMsgMalformed {
			t.Fatalf("Wrong response rejection code : got %d, want %d", rejection.RejectionCode,
				actions.RejectionsMsgMalformed)
		}

		js, _ = json.MarshalIndent(rejection, "", "  ")
		t.Logf("Rejection : %s", js)

		if !agent1ResponseTx3.Tx.TxIn[0].PreviousOutPoint.Hash.Equal(transferTxID) {
			t.Errorf("Wrong rejection input txid : got %s, want %s",
				agent1ResponseTx3.Tx.TxIn[0].PreviousOutPoint.Hash, transferTxID)
		}

		test.caches.Transactions.Release(ctx, rejectionTransaction.GetTxID())
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.caches.Caches.Contracts.Release(ctx, contractLockingScript2)
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
