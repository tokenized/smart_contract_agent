package funding

import (
	"context"
	"encoding/json"
	mathRand "math/rand"
	"testing"

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

func Test_Transfers_Random_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := agents.StartTestAgentWithInstrument(ctx, t)

	for i := 0; i < 1000; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteTransferRandom(t, ctx, agent, test, senderCount, false)
	}

	agents.StopTestAgent(ctx, t, test)
}

func Test_Transfers_Random_Multi_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := agents.StartTestAgentWithInstrument(ctx, t)

	for i := 0; i < 1000; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteTransferRandom(t, ctx, agent, test, senderCount, true)
	}

	agents.StopTestAgent(ctx, t, test)
}

func Test_Transfers_Random_Multi_Contract_P2PKH(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	test := agents.StartTestData(ctx, t)

	broadcaster1 := state.NewMockTxBroadcaster()
	broadcaster2 := state.NewMockTxBroadcaster()
	agentsConfig := agents.DefaultConfig()

	contractKey1, contractLockingScript1, adminKey1, adminLockingScript1, contract1, instrument1 := state.MockInstrument(ctx,
		&test.Caches.TestCaches)
	_, feeLockingScript1, _ := state.MockKey()

	agentData1 := agents.AgentData{
		Key:                contractKey1,
		LockingScript:      contractLockingScript1,
		MinimumContractFee: contract1.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript1,
		IsActive:           true,
	}

	agent1, err := agents.NewAgent(ctx, agentData1, agentsConfig, test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster1, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData1 := &agents.TestAgentData{
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

	agentData2 := agents.AgentData{
		Key:                contractKey2,
		LockingScript:      contractLockingScript2,
		MinimumContractFee: contract2.Formation.ContractFee,
		FeeLockingScript:   feeLockingScript2,
		IsActive:           true,
	}

	agent2, err := agents.NewAgent(ctx, agentData2, agentsConfig, test.Caches.Caches,
		test.Caches.Transactions, test.Caches.Services, test.Locker, test.Store, broadcaster2, nil,
		nil, nil, nil, test.PeerChannelsFactory, test.PeerChannelResponses)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	agentTestData2 := &agents.TestAgentData{
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

	for i := 0; i < 1; i++ {
		senderCount := mathRand.Intn(10) + 1
		MockExecuteTransferMultiContractRandom(t, ctx, test, agentTestData1, agentTestData2,
			senderCount, false)
	}

	agent1.Release(ctx)
	agent2.Release(ctx)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript1, instrument1.InstrumentCode)
	test.Caches.Caches.Instruments.Release(ctx, contractLockingScript2, instrument2.InstrumentCode)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript1)
	test.Caches.Caches.Contracts.Release(ctx, contractLockingScript2)
	agents.StopTestAgent(ctx, t, test)
}

func MockExecuteTransferRandom(t *testing.T, ctx context.Context, agent *agents.Agent,
	test *agents.TestData, senderCount int, useMulti bool) {

	var balances []*state.MockBalance
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

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	contractFees := []*ContractFee{
		{
			LockingScript: test.ContractLockingScript,
			ActionFee:     test.Contract.Formation.ContractFee,
			// ParticipantFee uint64         // satoshis per participant fee
		},
	}

	responseFunding, err := TransferResponseFunding(tx, contractFees, feeRate, dustFeeRate, true)
	if err != nil {
		t.Fatalf("Failed to calculate transfer response funding : %s", err)
	}

	for _, contract := range responseFunding.Contracts {
		if err := tx.SetOutputToDust(uint32(contract.Index)); err != nil {
			t.Fatalf("Failed to set contract output to dust : %s", err)
		}

		if err := tx.AddValueToOutput(uint32(contract.Index), contract.Value()); err != nil {
			t.Fatalf("Failed to add value to contract output : %s", err)
		}
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
		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)

		js, _ := json.MarshalIndent(responseFunding, "", "  ")
		t.Logf("Response funding : %s", js)

		t.Logf("Estimated settlement size : %d", responseFunding.EstimatedPayloadSize())
		t.Fatalf("Missing settlement action")
	}

	t.Logf("Funding overage (senders %d, recievers %d) (fee: %d, actual %d) : %d", senderCount,
		len(instrumentTransfer.InstrumentReceivers), test.Contract.Formation.ContractFee,
		feeValue, feeValue-test.Contract.Formation.ContractFee)

	if feeValue < test.Contract.Formation.ContractFee ||
		feeValue-test.Contract.Formation.ContractFee > feeValue/5 {
		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)

		js, _ := json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)
		t.Logf("Settlement script : %s", settlementScript)
		t.Logf("Settlement script : %x", []byte(settlementScript))
		t.Logf("Settlement size : %d", settlementSize)

		js, _ = json.MarshalIndent(responseFunding, "", "  ")
		t.Logf("Response funding : %s", js)

		t.Logf("Estimated settlement size : %d", responseFunding.EstimatedPayloadSize())
		t.Fatalf("Wrong funding : got %d, want >= %d",
			feeValue, test.Contract.Formation.ContractFee)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())
}

func MockExecuteTransferMultiContractRandom(t *testing.T, ctx context.Context,
	test *agents.TestData, agent1, agent2 *agents.TestAgentData, senderCount int,
	useMulti bool) {

	var balances1, balances2 []*state.MockBalance
	if useMulti {
		signerCount := mathRand.Intn(5) + 1
		signerThreshold := mathRand.Intn(signerCount) + 1
		balances1 = state.MockBalancesMultiSig(ctx, &test.Caches.TestCaches, agent1.Contract,
			agent1.Instrument, signerThreshold, signerCount, senderCount)

		balances2 = state.MockBalancesMultiSig(ctx, &test.Caches.TestCaches, agent2.Contract,
			agent2.Instrument, signerThreshold, signerCount, senderCount)
	} else {
		balances1 = state.MockBalances(ctx, &test.Caches.TestCaches, agent1.Contract,
			agent1.Instrument, senderCount)
		balances2 = state.MockBalances(ctx, &test.Caches.TestCaches, agent2.Contract,
			agent2.Instrument, senderCount)
	}

	instrumentTransfer1 := &actions.InstrumentTransferField{
		ContractIndex:  0,
		InstrumentType: string(agent1.Instrument.InstrumentType[:]),
		InstrumentCode: agent1.Instrument.InstrumentCode[:],
	}

	instrumentTransfer2 := &actions.InstrumentTransferField{
		ContractIndex:  1,
		InstrumentType: string(agent2.Instrument.InstrumentType[:]),
		InstrumentCode: agent2.Instrument.InstrumentCode[:],
	}

	transfer := &actions.Transfer{
		Instruments: []*actions.InstrumentTransferField{instrumentTransfer1, instrumentTransfer2},
	}

	feeRate := float64(0.05)
	dustFeeRate := float64(0.0)

	tx := txbuilder.NewTxBuilder(float32(feeRate), float32(dustFeeRate))
	etx := &expanded_tx.ExpandedTx{
		Tx: tx.MsgTx,
	}

	var keys []bitcoin.Key
	var spentOutputs []*expanded_tx.Output

	sentQuantity1 := uint64(0)
	for _, balance := range balances1 {
		keys = append(keys, balance.Keys...)

		// Add sender
		instrumentTransfer1.InstrumentSenders = append(instrumentTransfer1.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})
		sentQuantity1 += balance.Quantity

		// Add input
		inputTx, outpoint := state.MockOutPointTx(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
			Tx: inputTx,
		})
	}

	// Add receivers
	for {
		quantity := uint64(mathRand.Intn(int(sentQuantity1))) + 1000000
		if quantity > sentQuantity1 {
			quantity = sentQuantity1
		}

		_, _, ra := state.MockKey()
		instrumentTransfer1.InstrumentReceivers = append(instrumentTransfer1.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		sentQuantity1 -= quantity
		if sentQuantity1 == 0 {
			break
		}
	}

	sentQuantity2 := uint64(0)
	for _, balance := range balances2 {
		keys = append(keys, balance.Keys...)

		// Add sender
		instrumentTransfer2.InstrumentSenders = append(instrumentTransfer2.InstrumentSenders,
			&actions.QuantityIndexField{
				Quantity: balance.Quantity,
				Index:    uint32(len(tx.MsgTx.TxIn)),
			})
		sentQuantity2 += balance.Quantity

		// Add input
		inputTx, outpoint := state.MockOutPointTx(balance.LockingScript, 1)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: balance.LockingScript,
			Value:         1,
		})

		if err := tx.AddInput(*outpoint, balance.LockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
			Tx: inputTx,
		})
	}

	// Add receivers
	for {
		quantity := uint64(mathRand.Intn(int(sentQuantity2))) + 1000000
		if quantity > sentQuantity2 {
			quantity = sentQuantity2
		}

		_, _, ra := state.MockKey()
		instrumentTransfer2.InstrumentReceivers = append(instrumentTransfer2.InstrumentReceivers,
			&actions.InstrumentReceiverField{
				Address:  ra.Bytes(),
				Quantity: quantity,
			})

		sentQuantity2 -= quantity
		if sentQuantity2 == 0 {
			break
		}
	}

	// Add contract outputs
	if err := tx.AddOutput(agent1.ContractLockingScript, agent1.Contract.Formation.ContractFee,
		false, false); err != nil {
		t.Fatalf("Failed to add contract  1output : %s", err)
	}
	if err := tx.AddOutput(agent2.ContractLockingScript, agent2.Contract.Formation.ContractFee,
		false, false); err != nil {
		t.Fatalf("Failed to add contract 2 output : %s", err)
	}

	// Add action output
	transferScript, err := protocol.Serialize(transfer, true)
	if err != nil {
		t.Fatalf("Failed to serialize transfer action : %s", err)
	}

	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	contractFees := []*ContractFee{
		{
			LockingScript: agent1.ContractLockingScript,
			ActionFee:     agent1.Contract.Formation.ContractFee,
			// ParticipantFee uint64         // satoshis per participant fee
		},
		{
			LockingScript: agent2.ContractLockingScript,
			ActionFee:     agent2.Contract.Formation.ContractFee,
			// ParticipantFee uint64         // satoshis per participant fee
		},
	}

	responseFunding, err := TransferResponseFunding(tx, contractFees, feeRate, dustFeeRate, true)
	if err != nil {
		t.Fatalf("Failed to calculate transfer response funding : %s", err)
	}

	for _, contract := range responseFunding.Contracts {
		if err := tx.SetOutputToDust(uint32(contract.Index)); err != nil {
			t.Fatalf("Failed to set contract output to dust : %s", err)
		}

		if err := tx.AddValueToOutput(uint32(contract.Index), contract.Value()); err != nil {
			t.Fatalf("Failed to add value to contract output : %s", err)
		}
	}

	if responseFunding.Boomerang > 0 {
		if err := tx.AddOutput(agent1.ContractLockingScript, responseFunding.Boomerang, false,
			false); err != nil {
			t.Fatalf("Failed to add boomerang output : %s", err)
		}
	}

	actualFee := tx.ActualFee()
	estimatedFee := tx.EstimatedFee()
	if actualFee < int64(estimatedFee) {
		// Add funding
		funding := uint64(-actualFee) + estimatedFee + uint64(mathRand.Intn(10000)) +
			fees.EstimateFeeValue(txbuilder.MaximumP2PKHInputSize, feeRate)
		key, lockingScript, _ := state.MockKey()
		keys = append(keys, key)
		inputTx, outpoint := state.MockOutPointTx(lockingScript, funding)
		spentOutputs = append(spentOutputs, &expanded_tx.Output{
			LockingScript: lockingScript,
			Value:         funding,
		})

		if err := tx.AddInput(*outpoint, lockingScript, funding); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		etx.Ancestors = append(etx.Ancestors, &expanded_tx.AncestorTx{
			Tx: inputTx,
		})
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

	responseTxs := agents.TestProcessTx(ctx, []*agents.TestAgentData{agent1, agent2}, etx)
	if len(responseTxs) == 0 {
		t.Fatalf("No response tx")
	}
	responseTx := responseTxs[len(responseTxs)-1]

	if len(responseTxs) < 2 {
		t.Fatalf("No signature request tx")
	}
	signatureRequestTx := responseTxs[len(responseTxs)-2]

	js, _ := json.MarshalIndent(responseFunding, "", "  ")
	t.Logf("Response funding : %s", js)

	for _, tx := range responseTxs {
		t.Logf("Response tx : %s", tx)
	}

	t.Logf("Boomerang overage : %d (%%%0.2f)", signatureRequestTx.Tx.TxOut[0].Value,
		(float64(signatureRequestTx.Tx.TxOut[0].Value)/float64(responseFunding.Boomerang))*100.0)

	// Find settlement action
	var settlement *actions.Settlement
	var settlementSize int
	var settlementScript bitcoin.Script
	var feeValue uint64
	for _, txout := range responseTx.Tx.TxOut {
		if txout.LockingScript.Equal(agent1.FeeLockingScript) {
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
		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)

		js, _ := json.MarshalIndent(responseFunding, "", "  ")
		t.Logf("Response funding : %s", js)

		t.Logf("Estimated settlement size : %d", responseFunding.EstimatedPayloadSize())
		t.Fatalf("Missing settlement action")
	}

	t.Logf("Funding overage (senders %d, recievers %d) (fee: %d, actual %d) : %d", senderCount*2,
		len(instrumentTransfer1.InstrumentReceivers)+len(instrumentTransfer1.InstrumentReceivers),
		agent1.Contract.Formation.ContractFee, feeValue,
		feeValue-agent1.Contract.Formation.ContractFee)

	if feeValue < agent1.Contract.Formation.ContractFee ||
		feeValue-agent1.Contract.Formation.ContractFee > feeValue/5 {
		t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
		t.Logf("Response Tx : %s", responseTx)

		js, _ := json.MarshalIndent(settlement, "", "  ")
		t.Logf("Settlement : %s", js)
		t.Logf("Settlement script : %s", settlementScript)
		t.Logf("Settlement script : %x", []byte(settlementScript))
		t.Logf("Settlement size : %d", settlementSize)

		js, _ = json.MarshalIndent(responseFunding, "", "  ")
		t.Logf("Response funding : %s", js)

		t.Logf("Estimated settlement size : %d", responseFunding.EstimatedPayloadSize())
		t.Fatalf("Wrong funding : got %d, want >= %d",
			feeValue, agent1.Contract.Formation.ContractFee)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))
	t.Logf("Response Tx : %s", responseTx)
	js, _ = json.MarshalIndent(settlement, "", "  ")
	t.Logf("Settlement : %s", js)
}
