package agents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Instruments_Definition(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, _ := state.MockContract(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, caches.Transactions, caches.Services, locker, store,
		broadcaster, nil, nil, nil, nil, peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	for i := 0; i < 10; i++ {
		couponPayload := &instruments.Coupon{
			RedeemingEntity:     "test.com",
			ValidFromTimestamp:  uint64(time.Now().UnixNano()),
			ExpirationTimestamp: uint64(time.Now().Add(time.Hour * 20000).UnixNano()),
			CouponName:          fmt.Sprintf("Test Coupon %d", i),
			TransfersPermitted:  true,
			FaceValue: &instruments.CurrencyValueField{
				Value:        100,
				CurrencyCode: instruments.CurrenciesAustralianDollar,
				Precision:    2,
			},
		}

		couponBuf := &bytes.Buffer{}
		if err := couponPayload.Serialize(couponBuf); err != nil {
			panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
		}

		definition := &actions.InstrumentDefinition{
			// InstrumentPermissions            []byte
			// EnforcementOrdersPermitted       bool
			// VotingRights                     bool
			// VoteMultiplier                   uint32
			// AdministrationProposal           bool
			// HolderProposal                   bool
			// InstrumentModificationGovernance uint32
			AuthorizedTokenQty: uint64(mathRand.Intn(100000)),
			InstrumentType:     instruments.CodeCoupon,
			InstrumentPayload:  couponBuf.Bytes(),
			TradeRestrictions:  []string{actions.PolitiesAustralia},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		// Add input
		outpoint := state.MockOutPoint(adminLockingScript, 1)
		spentOutputs := []*expanded_tx.Output{
			{
				LockingScript: adminLockingScript,
				Value:         1,
			},
		}

		if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add contract output
		if err := tx.AddOutput(contractLockingScript, 150, false, false); err != nil {
			t.Fatalf("Failed to add contract output : %s", err)
		}

		// Add action output
		definitionScript, err := protocol.Serialize(definition, true)
		if err != nil {
			t.Fatalf("Failed to serialize instrument definition action : %s", err)
		}

		definitionScriptOutputIndex := len(tx.Outputs)
		if err := tx.AddOutput(definitionScript, 0, false, false); err != nil {
			t.Fatalf("Failed to add instrument definition action output : %s", err)
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
			OutputIndex:         definitionScriptOutputIndex,
			Action:              definition,
		}}); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		caches.Transactions.Release(ctx, transaction.GetTxID())

		responseTx := broadcaster.GetLastTx()
		if responseTx == nil {
			t.Fatalf("No response tx")
		}

		t.Logf("Response Tx : %s", responseTx)

		// Find creation action
		var creation *actions.InstrumentCreation
		for _, txout := range responseTx.Tx.TxOut {
			action, err := protocol.Deserialize(txout.LockingScript, true)
			if err != nil {
				continue
			}

			if a, ok := action.(*actions.InstrumentCreation); ok {
				creation = a
				break
			}
		}

		if creation == nil {
			t.Fatalf("Missing creation action")
		}

		js, _ := json.MarshalIndent(creation, "", "  ")
		t.Logf("InstrumentCreation : %s", js)

		payload, err := instruments.Deserialize([]byte(creation.InstrumentType),
			creation.InstrumentPayload)
		if err != nil {
			t.Fatalf("Failed to deserialize payload : %s", err)
		}

		js, _ = json.MarshalIndent(payload, "", "  ")
		t.Logf("Instrument Payload : %s", js)

		couponPayloadResult, ok := payload.(*instruments.Coupon)
		if !ok {
			t.Errorf("Instrument payload not a coupon")
		}

		if couponPayloadResult.RedeemingEntity != "test.com" {
			t.Errorf("Wrong RedeemingEntity : got %s, want %s", couponPayloadResult.RedeemingEntity,
				"test.com")
		}

		if !couponPayloadResult.Equal(couponPayload) {
			t.Errorf("Coupon payload doesn't match")
		}
	}

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Instruments_Amendment_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, _, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, caches.Transactions, caches.Services, locker, store,
		broadcaster, nil, nil, nil, nil, peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty:         instrument.Creation.AuthorizedTokenQty + 10,
		EnforcementOrdersPermitted: true,
		InstrumentType:             instrument.Creation.InstrumentType,
		InstrumentPayload:          instrument.Creation.InstrumentPayload,
	}

	amendments, err := instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     instrument.Creation.InstrumentType,
		InstrumentCode:     instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	modificationScript, err := protocol.Serialize(modification, true)
	if err != nil {
		t.Fatalf("Failed to serialize instrument modification action : %s", err)
	}

	modificationScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(modificationScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add instrument modification action output : %s", err)
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
		OutputIndex:         modificationScriptOutputIndex,
		Action:              modification,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var creation *actions.InstrumentCreation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.InstrumentCreation); ok {
			creation = a
			break
		}
	}

	if creation == nil {
		t.Fatalf("Missing creation action")
	}

	js, _ = json.MarshalIndent(creation, "", "  ")
	t.Logf("InstrumentCreation : %s", js)

	payload, err := instruments.Deserialize([]byte(creation.InstrumentType),
		creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	js, _ = json.MarshalIndent(payload, "", "  ")
	t.Logf("Instrument Payload : %s", js)

	_, ok := payload.(*instruments.Currency)
	if !ok {
		t.Errorf("Instrument payload not a currency")
	}

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Instruments_Amendment_Payload(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, adminKey, adminLockingScript, _, instrument := state.MockInstrument(ctx,
		&caches.TestCaches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, caches.Transactions, caches.Services, locker, store,
		broadcaster, nil, nil, nil, nil, peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	payload, err := instruments.Deserialize([]byte(instrument.Creation.InstrumentType),
		instrument.Creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	currency, ok := payload.(*instruments.Currency)
	if !ok {
		t.Fatalf("Instrument payload is not currency")
	}

	currency.CurrencyCode = instruments.CurrenciesAustralianDollar

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty:         instrument.Creation.AuthorizedTokenQty,
		EnforcementOrdersPermitted: true,
		InstrumentType:             instrument.Creation.InstrumentType,
		InstrumentPayload:          currencyBuf.Bytes(),
	}

	amendments, err := instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     instrument.Creation.InstrumentType,
		InstrumentCode:     instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	modificationScript, err := protocol.Serialize(modification, true)
	if err != nil {
		t.Fatalf("Failed to serialize instrument modification action : %s", err)
	}

	modificationScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(modificationScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add instrument modification action output : %s", err)
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
		OutputIndex:         modificationScriptOutputIndex,
		Action:              modification,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var creation *actions.InstrumentCreation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.InstrumentCreation); ok {
			creation = a
			break
		}
	}

	if creation == nil {
		t.Fatalf("Missing creation action")
	}

	js, _ = json.MarshalIndent(creation, "", "  ")
	t.Logf("InstrumentCreation : %s", js)

	payloadResult, err := instruments.Deserialize([]byte(creation.InstrumentType),
		creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	js, _ = json.MarshalIndent(payloadResult, "", "  ")
	t.Logf("Instrument Payload : %s", js)

	currencyResult, ok := payloadResult.(*instruments.Currency)
	if !ok {
		t.Errorf("Instrument payload not a currency")
	}

	if currencyResult.CurrencyCode != instruments.CurrenciesAustralianDollar {
		t.Fatalf("Wrong currency code : got %s, want %s", currencyResult.CurrencyCode,
			instruments.CurrenciesAustralianDollar)
	}

	agent.Release(ctx)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Instruments_Amendment_Proposal(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)
	locker := locker.NewInlineLocker()

	votingSystems := []*actions.VotingSystemField{
		{
			Name:                    "Basic",
			VoteType:                "R", // Relative Threshold
			TallyLogic:              0,   // Standard
			ThresholdPercentage:     50,
			VoteMultiplierPermitted: false,
			HolderProposalFee:       0,
		},
	}

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContractWithVoteSystems(ctx,
		&caches.TestCaches, votingSystems)
	_, feeLockingScript, _ := state.MockKey()

	instrument := state.MockInstrumentOnly(ctx, &caches.TestCaches, contract)

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, caches.Transactions, caches.Services, locker, store,
		broadcaster, nil, nil, nil, nil, peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty: instrument.Creation.AuthorizedTokenQty + 10,
		InstrumentType:     instrument.Creation.InstrumentType,
		InstrumentPayload:  instrument.Creation.InstrumentPayload,
		VotingRights:       true,
	}

	amendments, err := instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	vote := MockVoteInstrumentAmendmentCompleted(ctx, caches,
		instrument.Creation.InstrumentType, instrument.Creation.InstrumentCode, adminLockingScript,
		contractLockingScript, 0, amendments)
	vote.Lock()
	voteTxID := *vote.VoteTxID

	js, _ = json.MarshalIndent(vote, "", "  ")
	t.Logf("Vote : %s", js)
	vote.Unlock()

	modification := &actions.InstrumentModification{
		InstrumentType:     instrument.Creation.InstrumentType,
		InstrumentCode:     instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
		RefTxID:            vote.ResultTxID[:],
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	modificationScript, err := protocol.Serialize(modification, true)
	if err != nil {
		t.Fatalf("Failed to serialize instrument modification action : %s", err)
	}

	modificationScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(modificationScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add instrument modification action output : %s", err)
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
		OutputIndex:         modificationScriptOutputIndex,
		Action:              modification,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var creation *actions.InstrumentCreation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.InstrumentCreation); ok {
			creation = a
			break
		}
	}

	if creation == nil {
		t.Fatalf("Missing creation action")
	}

	js, _ = json.MarshalIndent(creation, "", "  ")
	t.Logf("InstrumentCreation : %s", js)

	payload, err := instruments.Deserialize([]byte(creation.InstrumentType),
		creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	js, _ = json.MarshalIndent(payload, "", "  ")
	t.Logf("Instrument Payload : %s", js)

	_, ok := payload.(*instruments.Currency)
	if !ok {
		t.Errorf("Instrument payload not a currency")
	}

	agent.Release(ctx)
	caches.Caches.Votes.Release(ctx, contractLockingScript, voteTxID)
	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
