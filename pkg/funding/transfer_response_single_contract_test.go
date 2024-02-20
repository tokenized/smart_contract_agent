package funding

import (
	"context"
	"encoding/json"
	mathRand "math/rand"
	"testing"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/agents"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
	"github.com/tokenized/txbuilder/fees"
)

func Test_SingleContract_Transfers_Random_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := agents.StartTestAgentWithInstrumentWithContractFee(ctx, t, 0)

	for i := 0; i < 100; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteSingleContractTransferRandom(t, ctx, agent, test, senderCount, false)
	}

	agents.StopTestAgent(ctx, t, test)
}

func Test_SingleContract_Transfers_Random_Multi_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := agents.StartTestAgentWithInstrumentWithContractFee(ctx, t, 0)

	for i := 0; i < 100; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteSingleContractTransferRandom(t, ctx, agent, test, senderCount, true)
	}

	agents.StopTestAgent(ctx, t, test)
}

func Test_SingleContract_Transfers_Random_TransferFee_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	cache := cacher.NewSimpleCache(store)

	key, _ := bitcoin.GenerateKey(bitcoin.MainNet)
	ls, _ := key.LockingScript()
	agent, test := agents.StartTestAgentWithCacherWithInstrumentTransferFee(ctx, t, store, cache,
		5000, ls)

	for i := 0; i < 100; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteSingleContractTransferRandom(t, ctx, agent, test, senderCount, false)
	}

	agents.StopTestAgent(ctx, t, test)
}

func MockExecuteSingleContractTransferRandom(t *testing.T, ctx context.Context, agent *agents.Agent,
	test *agents.TestData, senderCount int, useMulti bool) {

	var balances []*state.MockBalanceData
	if useMulti {
		signerCount := mathRand.Intn(5) + 1
		signerThreshold := mathRand.Intn(signerCount) + 1
		balances = state.MockBalancesMultiSig(ctx, &test.Caches.TestCaches, test.Contract,
			test.Instrument, signerThreshold, signerCount, senderCount)
	} else {
		balances = state.MockBalances(ctx, &test.Caches.TestCaches, test.Contract, test.Instrument,
			senderCount)
	}

	instrumentTransfer := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(test.Instrument.InstrumentType[:]),
		InstrumentCode: test.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer},
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
	if err := tx.AddOutput(test.ContractLockingScript, 0, false, true); err != nil {
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
	var contractFeeValue uint64
	for _, txout := range responseTx.Tx.TxOut {
		if txout.LockingScript.Equal(test.FeeLockingScript) {
			contractFeeValue += txout.Value
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
	t.Logf("Response tx size (senders %d, receivers %d) : estimated %d, actual %d",
		senderCount, len(instrumentTransfer.InstrumentReceivers),
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
