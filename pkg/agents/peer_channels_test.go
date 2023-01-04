package agents

import (
	"bytes"
	"context"
	"encoding/json"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/channels"
	channelsExpandedTx "github.com/tokenized/channels/expanded_tx"
	envelopeV1 "github.com/tokenized/envelope/pkg/golang/envelope/v1"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/client"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/google/uuid"
)

func Test_Responder_Response(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	peerChannelsFactory := peer_channels.NewFactory()
	mockClient := peerChannelsFactory.MockClient()
	accountID, accountToken, err := mockClient.CreateAccount(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channels account : %s", err)
	}

	accountClient := peer_channels.NewMockAccountClient(mockClient, *accountID, *accountToken)

	peerChannelData, err := accountClient.CreateChannel(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channel : %s", err)
	}

	writePeerChannel, err := peer_channels.NewChannel(peer_channels.MockClientURL,
		peerChannelData.ID, peerChannelData.WriteToken)
	if err != nil {
		t.Fatalf("Failed new peer channel : %s", err)
	}

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:              contractKey,
		LockingScript:    contractLockingScript,
		ContractFee:      contract.Formation.ContractFee,
		FeeLockingScript: feeLockingScript,
		IsActive:         true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), caches.Caches, caches.Transactions,
		caches.Services, locker, store, broadcaster, nil, nil, nil, nil, peerChannelsFactory)
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

	var spentOutputs []*expanded_tx.Output

	// Add admin as sender
	quantity := uint64(mathRand.Intn(1000)) + 1

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
	_, _, receiverAddress := state.MockKey()

	instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
		&actions.InstrumentReceiverField{
			Address:  receiverAddress.Bytes(),
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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	requestTxID := *tx.MsgTx.TxHash()

	if err := agent.AddResponder(ctx, requestTxID, writePeerChannel); err != nil {
		t.Fatalf("Failed to add responder : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		AgentLockingScripts: []bitcoin.Script{contractLockingScript},
		OutputIndex:         transferScriptOutputIndex,
		Action:              transfer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, requestTxID)

	responseMessages, err := mockClient.GetMessages(ctx, peerChannelData.ID,
		peerChannelData.ReadToken, true, 5)
	if err != nil {
		t.Fatalf("Failed to get peer channel messages : %s", err)
	}

	js, _ := json.MarshalIndent(responseMessages, "", "  ")
	t.Logf("Response Messages : %s", js)

	if len(responseMessages) != 1 {
		t.Fatalf("Wrong response messages count : got %d, want %d", len(responseMessages), 1)
	}

	protocols := channels.NewProtocols(channelsExpandedTx.NewProtocol())
	payload, err := envelopeV1.Parse(bytes.NewReader(responseMessages[0].Payload))
	if err != nil {
		t.Fatalf("Failed to parse envelope : %s", err)
	}

	protocol := protocols.GetProtocol(payload.ProtocolIDs[0])
	if protocol == nil {
		t.Fatalf("Failed to get envelope protocol : %s", err)
	}

	msg, _, err := protocol.Parse(payload)
	if err != nil {
		t.Fatalf("Failed to parse protocol : %s", err)
	}

	cetx, ok := msg.(*channelsExpandedTx.ExpandedTxMessage)
	if !ok {
		t.Fatalf("Message is wrong type")
	}

	etx := expanded_tx.ExpandedTx(*cetx)
	t.Logf("Response tx : %s", etx.String())

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Responder_Request(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	peerChannelsFactory := peer_channels.NewFactory()
	mockClient := peerChannelsFactory.MockClient()
	accountID, accountToken, err := mockClient.CreateAccount(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channels account : %s", err)
	}

	accountClient := peer_channels.NewMockAccountClient(mockClient, *accountID, *accountToken)

	peerChannelData, err := accountClient.CreateChannel(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channel : %s", err)
	}

	writePeerChannel, err := peer_channels.NewChannel(peer_channels.MockClientURL,
		peerChannelData.ID, peerChannelData.WriteToken)
	if err != nil {
		t.Fatalf("Failed new peer channel : %s", err)
	}

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:              contractKey,
		LockingScript:    contractLockingScript,
		ContractFee:      contract.Formation.ContractFee,
		FeeLockingScript: feeLockingScript,
		IsActive:         true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), caches.Caches, caches.Transactions,
		caches.Services, locker, store, broadcaster, nil, nil, nil, nil, peerChannelsFactory)
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

	var spentOutputs []*expanded_tx.Output

	// Add admin as sender
	quantity := uint64(mathRand.Intn(1000)) + 1

	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: quantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	adminTx, outpoint := state.MockOutPointTx(adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	_, _, receiverAddress := state.MockKey()

	instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
		&actions.InstrumentReceiverField{
			Address:  receiverAddress.Bytes(),
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

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingTx, fundingOutpoint := state.MockOutPointTx(fundingLockingScript, 300)
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
	requestTxID := *tx.MsgTx.TxHash()

	requestEtx, err := expanded_tx.NewExpandedTxFromTransactionWithOutputs(tx)
	if err != nil {
		t.Fatalf("Failed to convert to expanded tx : %s", err)
	}

	requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
		Tx: adminTx,
	})

	requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
		Tx: fundingTx,
	})

	t.Logf("Created tx : %s", requestEtx)

	script, err := client.WrapRequest(requestEtx, writePeerChannel)
	if err != nil {
		t.Fatalf("Failed to serialize message : %s", err)
	}

	t.Logf("Request : %s", script)

	peerChannelsMessage := peer_channels.Message{
		Sequence:    1,
		Received:    time.Now(),
		ContentType: peer_channels.ContentTypeBinary,
		ChannelID:   uuid.New().String(),
		Payload:     bitcoin.Hex(script),
	}

	if err := agent.ProcessPeerChannelMessage(ctx, peerChannelsMessage); err != nil {
		t.Fatalf("Failed to process message : %s", err)
	}

	transaction, err := caches.Transactions.Get(ctx, requestTxID)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if transaction == nil {
		t.Fatalf("Request transaction not found")
	}

	if err := agent.Process(ctx, transaction, []Action{{
		AgentLockingScripts: []bitcoin.Script{contractLockingScript},
		OutputIndex:         transferScriptOutputIndex,
		Action:              transfer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, requestTxID)

	responseMessages, err := mockClient.GetMessages(ctx, peerChannelData.ID,
		peerChannelData.ReadToken, true, 5)
	if err != nil {
		t.Fatalf("Failed to get peer channel messages : %s", err)
	}

	js, _ := json.MarshalIndent(responseMessages, "", "  ")
	t.Logf("Response Messages : %s", js)

	if len(responseMessages) != 1 {
		t.Fatalf("Wrong response messages count : got %d, want %d", len(responseMessages), 1)
	}

	response, err := client.UnwrapResponse(bitcoin.Script(responseMessages[0].Payload))
	if err != nil {
		t.Fatalf("Failed to unwrap response : %s", err)
	}

	if response.Signature != nil {
		t.Errorf("Response should not have signature")
	}

	if response.Response != nil {
		t.Errorf("Response should not have response")
	}

	if response.Tx == nil {
		t.Fatalf("Missing response tx")
	}

	t.Logf("Response tx : %s", response.Tx.String())

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Responder_AlreadyProcessed(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	peerChannelsFactory := peer_channels.NewFactory()
	mockClient := peerChannelsFactory.MockClient()
	accountID, accountToken, err := mockClient.CreateAccount(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channels account : %s", err)
	}

	accountClient := peer_channels.NewMockAccountClient(mockClient, *accountID, *accountToken)

	peerChannelData, err := accountClient.CreateChannel(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channel : %s", err)
	}

	writePeerChannel, err := peer_channels.NewChannel(peer_channels.MockClientURL,
		peerChannelData.ID, peerChannelData.WriteToken)
	if err != nil {
		t.Fatalf("Failed new peer channel : %s", err)
	}

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:              contractKey,
		LockingScript:    contractLockingScript,
		ContractFee:      contract.Formation.ContractFee,
		FeeLockingScript: feeLockingScript,
		IsActive:         true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), caches.Caches, caches.Transactions,
		caches.Services, locker, store, broadcaster, nil, nil, nil, nil, peerChannelsFactory)
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

	var spentOutputs []*expanded_tx.Output

	// Add admin as sender
	quantity := uint64(mathRand.Intn(1000)) + 1

	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: quantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	adminTx, outpoint := state.MockOutPointTx(adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	_, _, receiverAddress := state.MockKey()

	instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
		&actions.InstrumentReceiverField{
			Address:  receiverAddress.Bytes(),
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

	transferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(transferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add transfer action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingTx, fundingOutpoint := state.MockOutPointTx(fundingLockingScript, 300)
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
	requestTxID := *tx.MsgTx.TxHash()

	requestEtx, err := expanded_tx.NewExpandedTxFromTransactionWithOutputs(tx)
	if err != nil {
		t.Fatalf("Failed to convert to expanded tx : %s", err)
	}

	requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
		Tx: adminTx,
	})

	requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
		Tx: fundingTx,
	})

	t.Logf("Created tx : %s", requestEtx)

	transaction, err := caches.Transactions.AddExpandedTx(ctx, requestEtx)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		AgentLockingScripts: []bitcoin.Script{contractLockingScript},
		OutputIndex:         transferScriptOutputIndex,
		Action:              transfer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, requestTxID)

	script, err := client.WrapRequest(requestEtx, writePeerChannel)
	if err != nil {
		t.Fatalf("Failed to serialize message : %s", err)
	}

	peerChannelsMessage := peer_channels.Message{
		Sequence:    1,
		Received:    time.Now(),
		ContentType: peer_channels.ContentTypeBinary,
		ChannelID:   uuid.New().String(),
		Payload:     bitcoin.Hex(script),
	}

	if err := agent.ProcessPeerChannelMessage(ctx, peerChannelsMessage); err != nil {
		t.Fatalf("Failed to process message : %s", err)
	}

	responseMessages, err := mockClient.GetMessages(ctx, peerChannelData.ID,
		peerChannelData.ReadToken, true, 5)
	if err != nil {
		t.Fatalf("Failed to get peer channel messages : %s", err)
	}

	js, _ := json.MarshalIndent(responseMessages, "", "  ")
	t.Logf("Response Messages : %s", js)

	if len(responseMessages) != 1 {
		t.Fatalf("Wrong response messages count : got %d, want %d", len(responseMessages), 1)
	}

	response, err := client.UnwrapResponse(bitcoin.Script(responseMessages[0].Payload))
	if err != nil {
		t.Fatalf("Failed to unwrap response : %s", err)
	}

	if response.Signature != nil {
		t.Errorf("Response should not have signature")
	}

	if response.Response != nil {
		t.Errorf("Response should not have response")
	}

	if response.Tx == nil {
		t.Fatalf("Missing response tx")
	}

	t.Logf("Response tx : %s", response.Tx.String())

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Responder_Reject(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	peerChannelsFactory := peer_channels.NewFactory()
	mockClient := peerChannelsFactory.MockClient()
	accountID, accountToken, err := mockClient.CreateAccount(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channels account : %s", err)
	}

	accountClient := peer_channels.NewMockAccountClient(mockClient, *accountID, *accountToken)

	peerChannelData, err := accountClient.CreateChannel(ctx)
	if err != nil {
		t.Fatalf("Failed to create peer channel : %s", err)
	}

	writePeerChannel, err := peer_channels.NewChannel(peer_channels.MockClientURL,
		peerChannelData.ID, peerChannelData.WriteToken)
	if err != nil {
		t.Fatalf("Failed new peer channel : %s", err)
	}

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agentData := AgentData{
		Key:              contractKey,
		LockingScript:    contractLockingScript,
		ContractFee:      contract.Formation.ContractFee,
		FeeLockingScript: feeLockingScript,
		IsActive:         true,
	}

	agent, err := NewAgent(ctx, agentData, DefaultConfig(), caches.Caches, caches.Transactions,
		caches.Services, locker, store, broadcaster, nil, nil, nil, nil, peerChannelsFactory)
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

	var spentOutputs []*expanded_tx.Output

	// Add admin as sender
	quantity := uint64(mathRand.Intn(1000)) + 1

	instrumentTransfer.InstrumentSenders = append(instrumentTransfer.InstrumentSenders,
		&actions.QuantityIndexField{
			Quantity: quantity,
			Index:    uint32(len(tx.MsgTx.TxIn)),
		})

	// Add input
	adminTx, outpoint := state.MockOutPointTx(adminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: adminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add receivers
	_, _, receiverAddress := state.MockKey()

	instrumentTransfer.InstrumentReceivers = append(instrumentTransfer.InstrumentReceivers,
		&actions.InstrumentReceiverField{
			Address:  receiverAddress.Bytes(),
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
	_, fundingOutpoint := state.MockOutPointTx(fundingLockingScript, 300)
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

	requestEtx, err := expanded_tx.NewExpandedTxFromTransactionWithOutputs(tx)
	if err != nil {
		t.Fatalf("Failed to convert to expanded tx : %s", err)
	}

	requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
		Tx: adminTx,
	})

	// Exclude funding ancestor.
	// requestEtx.Ancestors = append(requestEtx.Ancestors, &expanded_tx.AncestorTx{
	// 	Tx: fundingTx,
	// })

	t.Logf("Created tx : %s", requestEtx)

	script, err := client.WrapRequest(requestEtx, writePeerChannel)
	if err != nil {
		t.Fatalf("Failed to serialize message : %s", err)
	}

	peerChannelsMessage := peer_channels.Message{
		Sequence:    1,
		Received:    time.Now(),
		ContentType: peer_channels.ContentTypeBinary,
		ChannelID:   uuid.New().String(),
		Payload:     bitcoin.Hex(script),
	}

	if err := agent.ProcessPeerChannelMessage(ctx, peerChannelsMessage); err != nil {
		t.Fatalf("Failed to process message : %s", err)
	}

	responseMessages, err := mockClient.GetMessages(ctx, peerChannelData.ID,
		peerChannelData.ReadToken, true, 5)
	if err != nil {
		t.Fatalf("Failed to get peer channel messages : %s", err)
	}

	js, _ := json.MarshalIndent(responseMessages, "", "  ")
	t.Logf("Response Messages : %s", js)

	if len(responseMessages) != 1 {
		t.Fatalf("Wrong response messages count : got %d, want %d", len(responseMessages), 1)
	}

	response, err := client.UnwrapResponse(bitcoin.Script(responseMessages[0].Payload))
	if err != nil {
		t.Fatalf("Failed to unwrap response : %s", err)
	}

	if response.Signature == nil {
		t.Errorf("Response signature missing")
	}

	pk := contractKey.PublicKey()
	response.Signature.SetPublicKey(&pk)

	if err := response.Signature.Verify(); err != nil {
		t.Errorf("Invalid response signature : %s", err)
	}

	if response.Response == nil {
		t.Errorf("Response response missing")
	}

	t.Logf("Response : %s", response.Response)

	if !bytes.Equal(response.Response.CodeProtocolID[:], channelsExpandedTx.ProtocolID[:]) {
		t.Errorf("Wrong response protocol ID : got %s, want %s", response.Response.CodeProtocolID,
			channelsExpandedTx.ProtocolID)
	}

	if response.Tx != nil {
		t.Fatalf("Response should not have tx")
	}

	if response.TxID == nil {
		t.Fatalf("Response should have a txid")
	}

	requestTxID := requestEtx.TxID()
	if !response.TxID.Equal(&requestTxID) {
		t.Fatalf("Response should contain request tx : got %s, want %s", response.TxID, requestTxID)
	}

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
