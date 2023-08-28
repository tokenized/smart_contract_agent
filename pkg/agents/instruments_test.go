package agents

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
)

func Test_Instruments_Definition_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	for i := 0; i < 10; i++ {
		couponPayload := &instruments.DiscountCoupon{
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
			InstrumentType:     instruments.CodeDiscountCoupon,
			InstrumentPayload:  couponBuf.Bytes(),
			TradeRestrictions:  []string{actions.PolitiesAustralia},
		}

		tx := txbuilder.NewTxBuilder(0.05, 0.0)

		// Add input
		outpoint := state.MockOutPoint(test.adminLockingScript, 1)
		spentOutputs := []*expanded_tx.Output{
			{
				LockingScript: test.adminLockingScript,
				Value:         1,
			},
		}

		if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add contract output
		if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
			OutputIndex: definitionScriptOutputIndex,
			Action:      definition,
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

		responseTx := test.broadcaster.GetLastTx()
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

		couponPayloadResult, ok := payload.(*instruments.DiscountCoupon)
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

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_CRN_Definition_BadFaceValue(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	creditNotePayload := &instruments.CreditNote{
		// Name: "Australian Dollar Note", // deprecated
		FaceValue: &instruments.FixedCurrencyValueField{
			Value:        0,
			CurrencyCode: instruments.CurrenciesAustralianDollar,
		},
	}

	creditNoteBuf := &bytes.Buffer{}
	if err := creditNotePayload.Serialize(creditNoteBuf); err != nil {
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
		InstrumentType:     instruments.CodeCreditNote,
		InstrumentPayload:  creditNoteBuf.Bytes(),
		TradeRestrictions:  []string{actions.PolitiesAustralia},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: definitionScriptOutputIndex,
		Action:      definition,
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

		if a, ok := action.(*actions.Rejection); ok {
			rejection = a
			break
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	js, _ := json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_CRN_Definition_Reject_LowFee(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	creditNotePayload := &instruments.CreditNote{
		// Name: "Australian Dollar Note", // deprecated
		FaceValue: &instruments.FixedCurrencyValueField{
			Value:        0,
			CurrencyCode: instruments.CurrenciesAustralianDollar,
		},
	}

	creditNoteBuf := &bytes.Buffer{}
	if err := creditNotePayload.Serialize(creditNoteBuf); err != nil {
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
		InstrumentType:     instruments.CodeCreditNote,
		InstrumentPayload:  creditNoteBuf.Bytes(),
		TradeRestrictions:  []string{actions.PolitiesAustralia},
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 110, false, false); err != nil {
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
		OutputIndex: definitionScriptOutputIndex,
		Action:      definition,
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

		if a, ok := action.(*actions.Rejection); ok {
			rejection = a
			break
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	js, _ := json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty:         test.instrument.Creation.AuthorizedTokenQty + 10,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.instrument.Creation.InstrumentType,
		InstrumentPayload:          test.instrument.Creation.InstrumentPayload,
	}

	amendments, err := test.instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.instrument.Creation.InstrumentType,
		InstrumentCode:     test.instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
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

	responseTx := test.broadcaster.GetLastTx()
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

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_Payload(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	payload, err := instruments.Deserialize([]byte(test.instrument.Creation.InstrumentType),
		test.instrument.Creation.InstrumentPayload)
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
		AuthorizedTokenQty:         test.instrument.Creation.AuthorizedTokenQty,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.instrument.Creation.InstrumentType,
		InstrumentPayload:          currencyBuf.Bytes(),
	}

	amendments, err := test.instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.instrument.Creation.InstrumentType,
		InstrumentCode:     test.instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
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

	responseTx := test.broadcaster.GetLastTx()
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

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_Proposal(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")

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
	agent, test := StartTestAgentWithVoteSystems(ctx, t, votingSystems)

	instrument := state.MockInstrumentOnly(ctx, &test.caches.TestCaches, test.contract)

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

	vote := MockVoteInstrumentAmendmentCompleted(ctx, test.caches,
		instrument.Creation.InstrumentType, instrument.Creation.InstrumentCode, test.adminLockingScript,
		test.contractLockingScript, 0, amendments)
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
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
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

	responseTx := test.broadcaster.GetLastTx()
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

	if _, ok := payload.(*instruments.Currency); !ok {
		t.Errorf("Instrument payload not a currency")
	}

	test.caches.Caches.Votes.Release(ctx, test.contractLockingScript, voteTxID)
	test.caches.Caches.Instruments.Release(ctx, test.contractLockingScript, instrument.InstrumentCode)

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_CRN_FaceValue_Prohibited(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrumentCreditNote(ctx, t)

	payload, err := instruments.Deserialize([]byte(test.instrument.Creation.InstrumentType),
		test.instrument.Creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	creditNote, ok := payload.(*instruments.CreditNote)
	if !ok {
		t.Fatalf("Instrument payload is not credit note")
	}

	creditNote.FaceValue.CurrencyCode = instruments.CurrenciesAustralianDollar

	currencyBuf := &bytes.Buffer{}
	if err := creditNote.Serialize(currencyBuf); err != nil {
		panic(fmt.Sprintf("Failed to serialize instrument payload : %s", err))
	}

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty:         test.instrument.Creation.AuthorizedTokenQty,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.instrument.Creation.InstrumentType,
		InstrumentPayload:          currencyBuf.Bytes(),
	}

	amendments, err := test.instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.instrument.Creation.InstrumentType,
		InstrumentCode:     test.instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.adminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
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

		if a, ok := action.(*actions.Rejection); ok {
			rejection = a
			break
		}
	}

	if rejection == nil {
		t.Fatalf("Missing rejection action")
	}

	js, _ = json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	instrument, err := test.caches.Caches.Instruments.Get(ctx, test.contractLockingScript,
		test.instrument.InstrumentCode)
	if err != nil {
		t.Fatalf("Failed to get instrument : %s", err)
	}

	if instrument == nil {
		t.Fatalf("Missing instrument")
	}

	payloadResult, err := instruments.Deserialize([]byte(instrument.Creation.InstrumentType),
		instrument.Creation.InstrumentPayload)
	if err != nil {
		t.Fatalf("Failed to deserialize payload : %s", err)
	}

	js, _ = json.MarshalIndent(payloadResult, "", "  ")
	t.Logf("Instrument Payload : %s", js)

	creditNoteResult, ok := payloadResult.(*instruments.CreditNote)
	if !ok {
		t.Errorf("Instrument payload not a credit note")
	}

	if creditNoteResult.FaceValue.CurrencyCode != instruments.CurrenciesUnitedStatesDollar {
		t.Fatalf("Wrong currency code : got %s, want %s", creditNoteResult.FaceValue.CurrencyCode,
			instruments.CurrenciesUnitedStatesDollar)
	}

	StopTestAgent(ctx, t, test)
}
