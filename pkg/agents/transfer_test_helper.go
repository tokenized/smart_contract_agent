package agents

import (
	"bytes"
	"context"
	"encoding/json"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/messages"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/threads"
	"github.com/tokenized/txbuilder"
)

func RunTest_Transfers_Basic(ctx context.Context, t *testing.T, store *storage.MockStorage,
	cache cacher.Cacher) {

	agent, test := StartTestAgentWithCacherWithInstrument(ctx, t, store, cache)

	var receiverKeys []bitcoin.Key
	var receiverLockingScripts []bitcoin.Script
	var receiverQuantities []uint64
	for i := 0; i < 100; i++ {
		instrumentTransfer := &actions.InstrumentTransferField{
			ContractIndex:  0,
			InstrumentType: string(test.Instrument.InstrumentType[:]),
			InstrumentCode: test.Instrument.InstrumentCode[:],
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
		outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
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
		if err := tx.AddOutput(test.ContractLockingScript, 200, false, false); err != nil {
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

		if _, err := tx.Sign([]bitcoin.Key{test.AdminKey, fundingKey}); err != nil {
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

		test.Caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	receiverOffset := 0
	var finalLockingScripts []bitcoin.Script
	var finalQuantities []uint64
	for {
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
		if err := tx.AddOutput(test.ContractLockingScript, 200, false, false); err != nil {
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

		test.Caches.Transactions.Release(ctx, transaction.GetTxID())
	}

	// Check balances
	for i, lockingScript := range finalLockingScripts {
		balance, err := test.Caches.Caches.Balances.Get(ctx, test.ContractLockingScript,
			test.Instrument.InstrumentCode, lockingScript)
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

		test.Caches.Caches.Balances.Release(ctx, test.ContractLockingScript,
			test.Instrument.InstrumentCode, balance)
	}

	StopTestAgent(ctx, t, test)
}

func RunTest_Transfers_Multi_Basic(ctx context.Context, t *testing.T, store *storage.MockStorage,
	cache cacher.Cacher, transfersCount int, cacheExpireWait time.Duration) {

	test := StartTestDataWithCacher(ctx, t, store, cache)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster1, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData1 := &TestAgentData{
		Agent:                 agent1,
		Contract:              contract1,
		Instrument:            instrument1,
		ContractKey:           contractKey1,
		ContractLockingScript: contractLockingScript1,
		AdminKey:              adminKey1,
		AdminLockingScript:    adminLockingScript1,
		FeeLockingScript:      feeLockingScript1,
		Broadcaster:           broadcaster1,
		Caches:                test.Caches,
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster2, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData2 := &TestAgentData{
		Agent:                 agent2,
		Contract:              contract2,
		Instrument:            instrument2,
		ContractKey:           contractKey2,
		ContractLockingScript: contractLockingScript2,
		AdminKey:              adminKey2,
		AdminLockingScript:    adminLockingScript2,
		FeeLockingScript:      feeLockingScript2,
		Broadcaster:           broadcaster2,
		Caches:                test.Caches,
	}

	for i := 0; i < transfersCount; i++ {
		var receiver1Keys, receiver2Keys []bitcoin.Key
		var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
		var receiver1Quantities, receiver2Quantities []uint64

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
		quantity1 := uint64(mathRand.Intn(10000)) + 1

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

		quantity2 := uint64(mathRand.Intn(10000)) + 1

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
		remainingQuantity := quantity1
		for remainingQuantity > 0 {
			receivingQuantity := uint64(mathRand.Intn(1000)) + 1
			if receivingQuantity > remainingQuantity {
				receivingQuantity = remainingQuantity
			}
			remainingQuantity -= receivingQuantity

			key, lockingScript, ra := state.MockKey()
			receiver1Keys = append(receiver1Keys, key)
			receiver1LockingScripts = append(receiver1LockingScripts, lockingScript)
			receiver1Quantities = append(receiver1Quantities, receivingQuantity)

			instrumentTransfer1.InstrumentReceivers = append(instrumentTransfer1.InstrumentReceivers,
				&actions.InstrumentReceiverField{
					Address:  ra.Bytes(),
					Quantity: receivingQuantity,
				})
		}

		remainingQuantity = quantity2
		for remainingQuantity > 0 {
			receivingQuantity := uint64(mathRand.Intn(1000)) + 1
			if receivingQuantity > remainingQuantity {
				receivingQuantity = remainingQuantity
			}
			remainingQuantity -= receivingQuantity

			key, lockingScript, ra := state.MockKey()
			receiver2Keys = append(receiver2Keys, key)
			receiver2LockingScripts = append(receiver2LockingScripts, lockingScript)
			receiver2Quantities = append(receiver2Quantities, receivingQuantity)

			instrumentTransfer2.InstrumentReceivers = append(instrumentTransfer2.InstrumentReceivers,
				&actions.InstrumentReceiverField{
					Address:  ra.Bytes(),
					Quantity: receivingQuantity,
				})
		}

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
		transferTxID := tx.TxID()

		js, _ := json.MarshalIndent(transfer, "", "  ")
		t.Logf("Transfer : %s", js)

		transferTx := &expanded_tx.ExpandedTx{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		responseTxs := TestProcessTxSingle(ctx, []*TestAgentData{agentTestData1, agentTestData2},
			transferTx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		settlementRequestTx := responseTxs[0]

		t.Logf("Settlement request tx : %s", settlementRequestTx)

		// Find settlement request action
		var message *actions.Message
		for _, txout := range settlementRequestTx.Tx.TxOut {
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

		js, _ = json.MarshalIndent(message, "", "  ")
		t.Logf("Message : %s", js)

		msg, err := messages.Deserialize(message.MessageCode, message.MessagePayload)
		if err != nil {
			t.Fatalf("Failed to decode message payload : %s", err)
		}

		js, _ = json.MarshalIndent(msg, "", "  ")
		t.Logf("Settlement request : %s", js)

		time.Sleep(cacheExpireWait)

		responseTxs = TestProcessTxSingle(ctx, []*TestAgentData{agentTestData1, agentTestData2},
			settlementRequestTx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		signatureRequestTx := responseTxs[0]

		t.Logf("Signature request tx : %s", signatureRequestTx)

		// Find signature request action
		var message2 *actions.Message
		for _, txout := range signatureRequestTx.Tx.TxOut {
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

		messagePayload, err := messages.Deserialize(message2.MessageCode, message2.MessagePayload)
		if err != nil {
			t.Fatalf("Failed to deserialize message payload : %s", err)
		}

		sigRequestPayload, ok := messagePayload.(*messages.SignatureRequest)
		if !ok {
			t.Fatalf("Message payload not a sig request")
		}

		sigRequestPayloadTx := &wire.MsgTx{}
		if err := sigRequestPayloadTx.Deserialize(bytes.NewReader(sigRequestPayload.Payload)); err != nil {
			t.Fatalf("Failed to decode sig request tx : %s", err)
		}

		t.Logf("Sig request payload tx : %s", sigRequestPayloadTx)

		var sigSettlement *actions.Settlement
		for _, txout := range sigRequestPayloadTx.TxOut {
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

		// Check that the balances do have pending adjustement.
		balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
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
		test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
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
		balance, err = test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
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
		test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
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
		balance, err = test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
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
		test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
			instrument2.InstrumentCode, balance)

		if foundAdjustments == 0 {
			t.Fatalf("Pending adjustment not found before signature request")
		} else if foundAdjustments != 1 {
			t.Fatalf("Wrong number of pending adjustments found before signature request : got %d, want %d",
				foundAdjustments, 1)
		} else {
			t.Logf("Found pending adjustment before signature request")
		}

		time.Sleep(cacheExpireWait)

		responseTxs = TestProcessTxSingle(ctx, []*TestAgentData{agentTestData1, agentTestData2},
			signatureRequestTx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		settlementTx := responseTxs[0]

		t.Logf("Settlement tx : %s", settlementTx)

		var settlement *actions.Settlement
		for _, txout := range settlementTx.Tx.TxOut {
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

		// Check that the balances don't have any adjustements pending.
		balance, err = test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
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

		test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}

		// Check that the balances don't have any adjustements pending.
		balance, err = test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
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

		test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
			instrument1.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}

		time.Sleep(cacheExpireWait)

		responseTxs = TestProcessTxSingle(ctx, []*TestAgentData{agentTestData1, agentTestData2},
			settlementTx)
		if len(responseTxs) != 0 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 0)
		}

		// Check that the balances don't have any adjustements pending.
		balance, err = test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
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

		test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
			instrument2.InstrumentCode, balance)

		if foundAdjustments != 0 {
			t.Fatalf("Pending adjustment should not be found after signature request")
		} else {
			t.Logf("Did not find pending adjustments after signature request")
		}

		now := uint64(time.Now().UnixNano())
		for index, receiverLockingScript := range append(receiver1LockingScripts, adminLockingScript1) {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
				instrument1.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 1 balance : %s", err)
			}

			if balance == nil {
				t.Fatalf("Missing instrument 1 balance")
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 1 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 1 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if index < len(receiver1Quantities) && available != receiver1Quantities[index] {
				t.Fatalf("Wrong instrument 1 balance quantity : got %d, want %d", available,
					receiver1Quantities[index])
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
				instrument1.InstrumentCode, balance)
		}

		for index, receiverLockingScript := range append(receiver2LockingScripts, adminLockingScript2) {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
				instrument2.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 2 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 2 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 2 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if index < len(receiver2Quantities) && available != receiver2Quantities[index] {
				t.Fatalf("Wrong instrument 2 balance quantity : got %d, want %d", available,
					receiver2Quantities[index])
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
				instrument2.InstrumentCode, balance)
		}
	}

	instrument1Balances, err := test.Caches.Caches.Balances.List(ctx, contractLockingScript1,
		instrument1.InstrumentCode)
	if err != nil {
		t.Fatalf("Failed to list instrument 1 balances : %s", err)
	}

	for _, balance := range instrument1Balances {
		balance.Lock()
		js, _ := json.MarshalIndent(balance, "", "  ")
		adjustementCount := len(balance.Adjustments)
		balance.Unlock()

		t.Logf("Instrument 1 Balance : %s", js)

		if adjustementCount != 0 {
			t.Fatalf("Adjustment found on instrument 1 balance after completing transfers")
		}
	}

	test.Caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript1,
		instrument1.InstrumentCode, instrument1Balances)

	instrument2Balances, err := test.Caches.Caches.Balances.List(ctx, contractLockingScript2,
		instrument2.InstrumentCode)
	if err != nil {
		t.Fatalf("Failed to list instrument 1 balances : %s", err)
	}

	for _, balance := range instrument2Balances {
		balance.Lock()
		js, _ := json.MarshalIndent(balance, "", "  ")
		adjustementCount := len(balance.Adjustments)
		balance.Unlock()

		t.Logf("Instrument 2 Balance : %s", js)

		if adjustementCount != 0 {
			t.Fatalf("Adjustment found on instrument 2 balance after completing transfers")
		}
	}

	test.Caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript2,
		instrument2.InstrumentCode, instrument2Balances)

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

func RunTest_Transfers_Multi_Expire(ctx context.Context, t *testing.T, store *storage.MockStorage,
	cache cacher.Cacher) {

	test := StartTestDataWithCacher(ctx, t, store, cache)

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
		&test.Caches.TestCaches)

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, config, test.Caches.Caches, test.Caches.Transactions,
		test.Caches.Services, test.Locker, test.Store, broadcaster1, nil, nil, scheduler,
		test.mockStore, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData1 := &TestAgentData{
		Agent:                 agent1,
		Contract:              contract1,
		Instrument:            instrument1,
		ContractKey:           contractKey1,
		ContractLockingScript: contractLockingScript1,
		AdminKey:              adminKey1,
		AdminLockingScript:    adminLockingScript1,
		FeeLockingScript:      feeLockingScript,
		Broadcaster:           broadcaster1,
		Caches:                test.Caches,
	}

	test.mockStore.Add(agentData1)

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, config, test.Caches.Caches, test.Caches.Transactions,
		test.Caches.Services, test.Locker, test.Store, broadcaster2, nil, nil, scheduler,
		test.mockStore, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData2 := &TestAgentData{
		Agent:                 agent2,
		Contract:              contract2,
		Instrument:            instrument2,
		ContractKey:           contractKey2,
		ContractLockingScript: contractLockingScript2,
		AdminKey:              adminKey2,
		AdminLockingScript:    adminLockingScript2,
		FeeLockingScript:      feeLockingScript,
		Broadcaster:           broadcaster2,
		Caches:                test.Caches,
	}

	test.mockStore.Add(agentData2)

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

		etx := &expanded_tx.ExpandedTx{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		responseTxs := TestProcessTx(ctx, []*TestAgentData{agentTestData1}, etx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		settlementRequestTx := responseTxs[0]

		t.Logf("Agent 1 response tx 1 : %s", settlementRequestTx)

		// Find settlement request action
		var message *actions.Message
		for _, txout := range settlementRequestTx.Tx.TxOut {
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

		t.Logf("Sleeping to wait for expiration")
		time.Sleep(time.Millisecond * 300)

		agent1RejectTx := broadcaster1.GetLastTx()
		broadcaster1.ClearTxs()
		if agent1RejectTx == nil {
			t.Fatalf("No agent 1 response tx reject")
		}

		t.Logf("Agent 1 response tx reject : %s", agent1RejectTx)

		// Find signature request action
		var messageReject *actions.Rejection
		for _, txout := range agent1RejectTx.Tx.TxOut {
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

		responseTxs = TestProcessTx(ctx, []*TestAgentData{agentTestData2}, etx)
		if len(responseTxs) != 0 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 0)
		}

		responseTxs = TestProcessTx(ctx, []*TestAgentData{agentTestData2}, settlementRequestTx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		signatureRequestTx := responseTxs[0]

		t.Logf("Agent 2 response tx 2 : %s", signatureRequestTx)

		// Find signature request action
		var message2 *actions.Message
		for _, txout := range signatureRequestTx.Tx.TxOut {
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

		sigRequestPayloadTx := &wire.MsgTx{}
		if err := sigRequestPayloadTx.Deserialize(bytes.NewReader(sigRequestPayload.Payload)); err != nil {
			t.Fatalf("Failed to decode sig request tx : %s", err)
		}

		t.Logf("Sig request tx : %s", sigRequestPayloadTx)

		var sigSettlement *actions.Settlement
		for _, txout := range sigRequestPayloadTx.TxOut {
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

		responseTxs = TestProcessTx(ctx, []*TestAgentData{agentTestData1}, signatureRequestTx)
		if len(responseTxs) != 0 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 0)
		}

		t.Logf("Processing reject")
		responseTxs = TestProcessTx(ctx, []*TestAgentData{agentTestData2}, agent1RejectTx)
		if len(responseTxs) != 0 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 0)
		}

		time.Sleep(time.Millisecond * 300)

		agent2RejectTx := broadcaster1.GetLastTx()
		broadcaster2.ClearTxs()
		if agent2RejectTx == nil {
			t.Fatalf("No agent 2 response tx reject")
		}

		t.Logf("Agent 2 response tx reject : %s", agent2RejectTx)

		now := uint64(time.Now().UnixNano())
		for _, receiverLockingScript := range receiver1LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
				instrument1.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 1 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 1 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 1 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 1 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
				instrument1.InstrumentCode, balance)
		}

		for _, receiverLockingScript := range receiver2LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
				instrument2.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 2 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 2 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 2 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 2 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
				instrument2.InstrumentCode, balance)
		}
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

// Test_Transfers_Multi_Reject_First is a multi-contract transfer that is rejected by the first
// contract agent.
func RunTest_Transfers_Multi_Reject_First(ctx context.Context, t *testing.T,
	store *storage.MockStorage, cache cacher.Cacher) {

	test := StartTestDataWithCacher(ctx, t, store, cache)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster1, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData1 := &TestAgentData{
		Agent:                 agent1,
		Contract:              contract1,
		Instrument:            instrument1,
		ContractKey:           contractKey1,
		ContractLockingScript: contractLockingScript1,
		AdminKey:              adminKey1,
		AdminLockingScript:    adminLockingScript1,
		FeeLockingScript:      feeLockingScript1,
		Broadcaster:           broadcaster1,
		Caches:                test.Caches,
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster2, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData2 := &TestAgentData{
		Agent:                 agent2,
		Contract:              contract2,
		Instrument:            instrument2,
		ContractKey:           contractKey2,
		ContractLockingScript: contractLockingScript2,
		AdminKey:              adminKey2,
		AdminLockingScript:    adminLockingScript2,
		FeeLockingScript:      feeLockingScript2,
		Broadcaster:           broadcaster2,
		Caches:                test.Caches,
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

		etx := &expanded_tx.ExpandedTx{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		responseTxs := TestProcessTx(ctx, []*TestAgentData{agentTestData1, agentTestData2}, etx)
		if len(responseTxs) != 1 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 1)
		}
		responseTx1 := responseTxs[0]

		t.Logf("Agent 1 response tx 1 : %s", responseTx1)

		// Find rejection action
		var rejection *actions.Rejection
		for _, txout := range responseTx1.Tx.TxOut {
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

		now := uint64(time.Now().UnixNano())
		for _, receiverLockingScript := range receiver1LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
				instrument1.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 1 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 1 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 1 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 1 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
				instrument1.InstrumentCode, balance)
		}

		for _, receiverLockingScript := range receiver2LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
				instrument2.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 2 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 2 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 2 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 2 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
				instrument2.InstrumentCode, balance)
		}
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}

// Test_Transfers_Multi_Reject_Second is a multi-contract transfer that is rejected by the second
// contract agent.
func RunTest_Transfers_Multi_Reject_Second(ctx context.Context, t *testing.T,
	store *storage.MockStorage, cache cacher.Cacher) {

	test := StartTestDataWithCacher(ctx, t, store, cache)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := NewAgent(ctx, agentData1, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster1, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData1 := &TestAgentData{
		Agent:                 agent1,
		Contract:              contract1,
		Instrument:            instrument1,
		ContractKey:           contractKey1,
		ContractLockingScript: contractLockingScript1,
		AdminKey:              adminKey1,
		AdminLockingScript:    adminLockingScript1,
		FeeLockingScript:      feeLockingScript1,
		Broadcaster:           broadcaster1,
		Caches:                test.Caches,
	}

	contractKey2, contractLockingScript2, adminKey2, adminLockingScript2, contract2, instrument2 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript2, _ := state.MockKey()

	agentData2 := AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := NewAgent(ctx, agentData2, DefaultConfig(), test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster2, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData2 := &TestAgentData{
		Agent:                 agent2,
		Contract:              contract2,
		Instrument:            instrument2,
		ContractKey:           contractKey2,
		ContractLockingScript: contractLockingScript2,
		AdminKey:              adminKey2,
		AdminLockingScript:    adminLockingScript2,
		FeeLockingScript:      feeLockingScript2,
		Broadcaster:           broadcaster2,
		Caches:                test.Caches,
	}

	var receiver1Keys, receiver2Keys []bitcoin.Key
	var receiver1LockingScripts, receiver2LockingScripts []bitcoin.Script
	var receiver1Quantities, receiver2Quantities []uint64
	for i := 0; i < 100; i++ {
		var lockingScripts []bitcoin.Script

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
		lockingScripts = append(lockingScripts, adminLockingScript1)

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
		lockingScripts = append(lockingScripts, adminLockingScript2)

		if err := tx.AddInput(*outpoint2, adminLockingScript2, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add receivers
		key, lockingScript, ra := state.MockKey()
		receiver1Keys = append(receiver1Keys, key)
		receiver1LockingScripts = append(receiver1LockingScripts, lockingScript)
		lockingScripts = append(lockingScripts, lockingScript)

		instrumentTransfer1.InstrumentReceivers = append(instrumentTransfer1.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity1,
			})

		key, lockingScript, ra = state.MockKey()
		receiver2Keys = append(receiver2Keys, key)
		receiver2LockingScripts = append(receiver2LockingScripts, lockingScript)
		lockingScripts = append(lockingScripts, lockingScript)

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

		// transferScriptOutputIndex := len(tx.Outputs)
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

		transferTxID := tx.MsgTx.TxHash()

		etx := &expanded_tx.ExpandedTx{
			Tx:           tx.MsgTx,
			SpentOutputs: spentOutputs,
		}

		responseTxs := TestProcessTx(ctx, []*TestAgentData{agentTestData1, agentTestData2}, etx)
		if len(responseTxs) != 3 {
			t.Fatalf("Wrong response tx count : got %d, want %d", len(responseTxs), 3)
		}
		responseTx1 := responseTxs[0]

		t.Logf("Agent 1 response tx 1 : %s", responseTx1)

		// Find settlement request action
		var message *actions.Message
		for _, txout := range responseTx1.Tx.TxOut {
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

		responseTx2 := responseTxs[1]

		t.Logf("Agent 2 response tx 2 : %s", responseTx2)

		// Find rejection action
		var rejection *actions.Rejection
		for _, txout := range responseTx2.Tx.TxOut {
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

		responseTx3 := responseTxs[2]

		t.Logf("Agent 2 response tx 3 : %s", responseTx3)

		// Find rejection action
		rejection = nil
		for _, txout := range responseTx3.Tx.TxOut {
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

		if !responseTx3.Tx.TxIn[0].PreviousOutPoint.Hash.Equal(transferTxID) {
			t.Errorf("Wrong rejection input txid : got %s, want %s",
				responseTx3.Tx.TxIn[0].PreviousOutPoint.Hash, transferTxID)
		}

		now := uint64(time.Now().UnixNano())
		for _, receiverLockingScript := range receiver1LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript1,
				instrument1.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 1 balance : %s", err)
			}

			if balance == nil {
				t.Fatalf("Missing instrument 1 balance")
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 1 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 1 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 1 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript1,
				instrument1.InstrumentCode, balance)
		}

		for _, receiverLockingScript := range receiver2LockingScripts {
			t.Logf("Locking script : %s", receiverLockingScript)

			balance, err := test.Caches.Caches.Balances.Get(ctx, contractLockingScript2,
				instrument2.InstrumentCode, receiverLockingScript)
			if err != nil {
				t.Fatalf("Failed to get instrument 2 balance : %s", err)
			}

			if balance == nil {
				continue // balance may not have been created before failure
			}

			balance.Lock()
			js, _ := json.MarshalIndent(balance, "", "  ")
			t.Logf("Instrument 2 Balance : %s", js)

			if len(balance.Adjustments) > 0 {
				t.Fatalf("Instrument 2 Balance should not have adjustments")
			}

			available := balance.Available(now)
			if available != 0 {
				t.Fatalf("Wrong instrument 2 balance quantity : got %d, want %d", available, 0)
			}
			balance.Unlock()

			test.Caches.Caches.Balances.Release(ctx, contractLockingScript2,
				instrument2.InstrumentCode, balance)
		}
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	StopTestAgent(ctx, t, test)
}
