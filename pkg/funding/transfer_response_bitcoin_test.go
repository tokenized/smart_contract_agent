package funding

import (
	"context"
	"encoding/json"
	"fmt"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_interpreter/agent_bitcoin_transfer"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"
)

func Test_Transfers_Random_Bitcoin_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := agents.StartTestAgentWithInstrument(ctx, t)

	for i := 0; i < 100; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteTransferRandomBitcoin(t, ctx, agent, test, senderCount, false)
	}

	agents.StopTestAgent(ctx, t, test)
}

func MockExecuteTransferRandomBitcoin(t *testing.T, ctx context.Context, agent *agents.Agent,
	test *agents.TestData, senderCount int, useMulti bool) {

	var balances []*state.MockBalanceData
	var bitcoinBalances []*state.MockBalanceData
	if useMulti {
		signerCount := mathRand.Intn(5) + 1
		signerThreshold := mathRand.Intn(signerCount) + 1
		balances = state.MockBalancesMultiSig(ctx, &test.Caches.TestCaches, test.Contract,
			test.Instrument, signerThreshold, signerCount, senderCount)

		for i := 0; i < senderCount; i++ {
			keys := make([]bitcoin.Key, signerCount)
			publicKeys := make([]bitcoin.PublicKey, signerCount)
			for j := 0; j < signerCount; j++ {
				key, _, _ := state.MockKey()
				keys[j] = key
				publicKeys[j] = key.PublicKey()
			}

			template, err := bitcoin.NewMultiPKHTemplate(uint32(signerThreshold),
				uint32(signerCount))
			if err != nil {
				panic(fmt.Sprintf("multi-pkh template: %s", err))
			}

			lockingScript, err := template.LockingScript(publicKeys)
			if err != nil {
				panic(fmt.Sprintf("multi-pkh locking script: %s", err))
			}

			quantity := uint64(mathRand.Intn(100000000) + 1)

			bitcoinBalances = append(bitcoinBalances, &state.MockBalanceData{
				Keys:          keys,
				LockingScript: lockingScript,
				Quantity:      quantity,
			})
		}
	} else {
		balances = state.MockBalances(ctx, &test.Caches.TestCaches, test.Contract, test.Instrument,
			senderCount)

		for i := 0; i < senderCount; i++ {
			key, lockingScript, _ := state.MockKey()
			quantity := uint64(mathRand.Intn(100000000) + 1)

			bitcoinBalances = append(bitcoinBalances, &state.MockBalanceData{
				Keys:          []bitcoin.Key{key},
				LockingScript: lockingScript,
				Quantity:      quantity,
			})
		}
	}

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	bitcoinTransfer := &actions.InstrumentTransferField{
		InstrumentType: protocol.BSVInstrumentID,
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer, bitcoinTransfer},
	}

	feeRate := float64(0.05)
	dustFeeRate := float64(0.0)

	tx := txbuilder.NewTxBuilder(float32(feeRate), float32(dustFeeRate))
	var inputLockingScriptSizes []int

	var keys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output
	sentQuantity := uint64(0)
	for _, balance := range balances {
		keys = append(keys, balance.Keys...)

		// Add sender
		instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})
		sentQuantity += balance.Quantity

		// Add input
		outpoint := state.MockOutPoint(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
		inputLockingScriptSizes = append(inputLockingScriptSizes, len(balance.LockingScript))
	}

	// Add receivers
	for {
		quantity := uint64(mathRand.Intn(int(sentQuantity))) + 1000000
		if quantity > sentQuantity {
			quantity = sentQuantity
		}

		_, _, ra := state.MockKey()
		instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		sentQuantity -= quantity
		if sentQuantity == 0 {
			break
		}
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, test.Contract.Formation.ContractFee,
		false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	bitcoinSentQuantity := uint64(0)
	for _, balance := range bitcoinBalances {
		keys = append(keys, balance.Keys...)

		// Add sender
		bitcoinTransfer.InstrumentSenders = append(bitcoinTransfer.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})
		bitcoinSentQuantity += balance.Quantity

		key, lockingScript, _ := state.MockKey()
		keys = append(keys, key)
		outpoint := state.MockOutPoint(lockingScript, balance.Quantity)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: lockingScript,
			Value:         balance.Quantity,
		})

		if err := tx.AddInput(*outpoint, lockingScript, balance.Quantity); err != nil {
			t.Fatalf("Failed to add agent bitcoin transfer input : %s", err)
		}
		inputLockingScriptSizes = append(inputLockingScriptSizes, len(lockingScript))
	}

	// Add receivers
	for {
		quantity := uint64(mathRand.Intn(int(bitcoinSentQuantity))) + 1000000
		if quantity > bitcoinSentQuantity {
			quantity = bitcoinSentQuantity
		}

		_, lockingScript, ra := state.MockKey()
		bitcoinTransfer.InstrumentReceivers = append(bitcoinTransfer.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		agentScript, err := agent_bitcoin_transfer.CreateScript(test.ContractLockingScript,
			lockingScript, bitcoinBalances[0].LockingScript, quantity,
			bitcoinBalances[0].LockingScript, uint32(time.Now().Unix())+1000)
		if err != nil {
			t.Fatalf("Failed to generate agent bitcoin transfer script : %s", err)
		}

		if err := tx.AddOutput(agentScript, quantity, false, false); err != nil {
			t.Fatalf("Failed to add agent bitcoin transfer output : %s", err)
		}

		bitcoinSentQuantity -= quantity
		if bitcoinSentQuantity == 0 {
			break
		}
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

	transferFee := uint64(0)
	transferFeeLockingScriptSize := 0
	if test.Instrument.Creation.TransferFee != nil && test.Instrument.Creation.TransferFee.Quantity > 0 {
		transferFee = test.Instrument.Creation.TransferFee.Quantity

		ra, err := bitcoin.DecodeRawAddress(test.Instrument.Creation.TransferFee.Address)
		if err != nil {
			t.Fatalf("Failed to decode transfer fee address : %s", err)
		}

		ls, err := ra.LockingScript()
		if err != nil {
			t.Fatalf("Failed to generate transfer fee locking script : %s", err)
		}

		transferFeeLockingScriptSize = len(ls)
	}

	contractData := Contracts{
		{
			LockingScript: test.Contract.LockingScript,
			ActionFee:     test.Contract.Formation.ContractFee,
			Instruments: Instruments{
				{
					Code:                         test.Instrument.InstrumentCode[:],
					TransferFee:                  transferFee,
					TransferFeeLockingScriptSize: transferFeeLockingScriptSize,
				},
			},
		},
	}

	feeData, boomerang, err := CalculateTransferResponseFee(tx.MsgTx, inputLockingScriptSizes,
		feeRate, dustFeeRate, contractData, true)
	if err != nil {
		t.Fatalf("Failed to calculate contract fees : %s", err)
	}

	if err := UpdateTransaction(tx.MsgTx, feeData, boomerang); err != nil {
		t.Fatalf("Failed to update transaction : %s", err)
	}

	actualFee := tx.ActualFee()
	estimatedFee := tx.EstimatedFee()
	if actualFee < int64(estimatedFee) {
		// Add funding
		funding := uint64(-actualFee) + estimatedFee + uint64(mathRand.Intn(10000)) +
			fees.EstimateFeeValue(txbuilder.MaximumP2PKHInputSize, feeRate)
		key, lockingScript, _ := state.MockKey()
		keys = append(keys, key)
		outpoint := state.MockOutPoint(lockingScript, funding)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: lockingScript,
			Value:         funding,
		})

		if err := tx.AddInput(*outpoint, lockingScript, funding); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}
	} else {
		t.Logf("Not funding : %d + %d", actualFee, estimatedFee)
	}

	_, changeLockingScript, _ := state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign(keys); err != nil {
		t.Logf("Actual fee : %d", actualFee)
		t.Logf("Estimated fee : %d", estimatedFee)
		t.Logf("Tx : %s", tx.String(bitcoin.MainNet))
		t.Fatalf("Failed to sign tx : %s", err)
	}

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []agents.Action{{
		OutputIndex: transferScriptOutputIndex,
		Action:      transfer,
		Agents: []agents.ActionAgent{
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

	// Find settlement action
	var settlement *actions.Settlement
	var settlementSize int
	var settlementScript bitcoin.Script
	var feeValue uint64
	for _, txout := range responseTx.Tx.TxOut {
		if txout.LockingScript.Equal(test.FeeLockingScript) {
			feeValue += txout.Value
			continue
		}

		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		s, ok := action.(*actions.Settlement)
		if ok {
			settlementScript = txout.LockingScript
			settlementSize = len(txout.LockingScript)
			settlement = s
		}
	}

	if settlement == nil {
		t.Logf("Request Tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)
		t.Fatalf("Missing settlement action")
	}

	responseTxSize := responseTx.Tx.SerializeSize()
	if feeData[0].responseSize+feeData[0].payloadSize < responseTxSize {
		t.Logf("Request Tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)
		t.Logf("Response payload size : estimated %d, actual %d", feeData[0].payloadSize,
			settlementSize)
		t.Fatalf("Estimated response size is too low : got %d, actual %d",
			feeData[0].responseSize+feeData[0].payloadSize, responseTxSize)
	}
	if feeData[0].responseSize+feeData[0].payloadSize > responseTxSize*2 {
		t.Logf("Request Tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)
		t.Logf("Response payload size : estimated %d, actual %d", feeData[0].payloadSize,
			settlementSize)
		t.Fatalf("Estimated response size is too high : got %d, actual %d",
			feeData[0].responseSize+feeData[0].payloadSize, responseTxSize)
	}
	t.Logf("Response tx size : estimated %d, actual %d",
		feeData[0].responseSize+feeData[0].payloadSize, responseTxSize)

	if feeData[0].payloadSize < settlementSize {
		t.Logf("Request Tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)
		t.Fatalf("Estimated response payload size is too low : got %d, actual %d",
			feeData[0].payloadSize, settlementSize)
	}
	if feeData[0].payloadSize > settlementSize*2 {
		t.Logf("Request Tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)
		t.Fatalf("Estimated response payload size is too high : got %d, actual %d",
			feeData[0].payloadSize, settlementSize)
	}
	t.Logf("Response payload size (senders %d, receivers %d) : estimated %d, actual %d",
		senderCount, len(instrumentTransfer.InstrumentReceivers), feeData[0].payloadSize,
		settlementSize)

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
	t.Logf("Response Tx : %s", responseTx)

	js, _ := json.MarshalIndent(settlement, "", "  ")
	t.Logf("Settlement : %s", js)
	t.Logf("Settlement script : %s", settlementScript)
	t.Logf("Settlement script : %x", []byte(settlementScript))
	t.Logf("Settlement size : %d", settlementSize)

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())
}
