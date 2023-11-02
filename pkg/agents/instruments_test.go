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
	"github.com/tokenized/smart_contract_agent/pkg/statistics"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
)

func Test_Instruments_Definition_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	instrumentCount := 10

	for i := 0; i < instrumentCount; i++ {
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
		outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
		spentOutputs := []*expanded_tx.Output{
			{
				LockingScript: test.AdminLockingScript,
				Value:         1,
			},
		}

		if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
			t.Fatalf("Failed to add input : %s", err)
		}

		// Add contract output
		if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
			OutputIndex: definitionScriptOutputIndex,
			Action:      definition,
			Agents: []ActionAgent{
				{
					LockingScript: test.ContractLockingScript,
					IsRequest:     true,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process transaction : %s", err)
		}

		test.Caches.Transactions.Release(ctx, transaction.GetTxID())

		responseTx := test.Broadcaster.GetLastTx()
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

		time.Sleep(time.Millisecond) // wait for statistics to process

		stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch contract statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		stats.Lock()

		statAction := stats.GetAction(actions.CodeInstrumentDefinition)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != uint64(i+1) {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, i+1)
		}

		if statAction.RejectedCount != 0 {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, 0)
		}

		stats.Unlock()

		var instrumentCode state.InstrumentCode
		copy(instrumentCode[:], creation.InstrumentCode)
		stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
			state.CalculateContractHash(test.ContractLockingScript), instrumentCode,
			uint64(time.Now().UnixNano()))
		if err != nil {
			t.Fatalf("Failed to fetch instrument statistics : %s", err)
		}

		js, _ = json.MarshalIndent(stats, "", "  ")
		t.Logf("Stats : %s", js)

		stats.Lock()

		statAction = stats.GetAction(actions.CodeInstrumentDefinition)
		if statAction == nil {
			t.Fatalf("Missing statistics action for code")
		}

		if statAction.Count != 1 {
			t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
		}

		if statAction.RejectedCount != 0 {
			t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
				statAction.RejectedCount, 0)
		}

		stats.Unlock()
	}

	// Verify instrument count and contract is modified
	if test.Contract.InstrumentCount != uint64(instrumentCount) {
		t.Fatalf("Wrong contract instrument count : got %d, want %d", test.Contract.InstrumentCount,
			instrumentCount)
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
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: definitionScriptOutputIndex,
		Action:      definition,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentDefinition)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

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
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 117, false, false); err != nil {
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
		OutputIndex: definitionScriptOutputIndex,
		Action:      definition,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentDefinition)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	newDefinition := &actions.InstrumentDefinition{
		AuthorizedTokenQty:         test.Instrument.Creation.AuthorizedTokenQty + 10,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.Instrument.Creation.InstrumentType,
		InstrumentPayload:          test.Instrument.Creation.InstrumentPayload,
	}

	amendments, err := test.Instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.Instrument.Creation.InstrumentType,
		InstrumentCode:     test.Instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)
	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), instrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction = stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_Payload(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	payload, err := instruments.Deserialize([]byte(test.Instrument.Creation.InstrumentType),
		test.Instrument.Creation.InstrumentPayload)
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
		AuthorizedTokenQty:         test.Instrument.Creation.AuthorizedTokenQty,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.Instrument.Creation.InstrumentType,
		InstrumentPayload:          currencyBuf.Bytes(),
	}

	amendments, err := test.Instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.Instrument.Creation.InstrumentType,
		InstrumentCode:     test.Instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)
	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), instrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction = stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

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

	instrument := state.MockInstrumentOnly(ctx, &test.Caches.TestCaches, test.Contract)

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

	vote := MockVoteInstrumentAmendmentCompleted(ctx, test.Caches,
		instrument.Creation.InstrumentType, instrument.Creation.InstrumentCode, test.AdminLockingScript,
		test.ContractLockingScript, 0, amendments)
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
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	test.Caches.Caches.Votes.Release(ctx, test.ContractLockingScript, voteTxID)
	test.Caches.Caches.Instruments.Release(ctx, test.ContractLockingScript, instrument.InstrumentCode)

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	var instrumentCode state.InstrumentCode
	copy(instrumentCode[:], creation.InstrumentCode)
	stats, err = statistics.FetchInstrumentValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), instrumentCode,
		uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch instrument statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction = stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 0 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 0)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)
}

func Test_Instruments_Amendment_CRN_FaceValue_Prohibited(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrumentCreditNote(ctx, t)

	payload, err := instruments.Deserialize([]byte(test.Instrument.Creation.InstrumentType),
		test.Instrument.Creation.InstrumentPayload)
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
		AuthorizedTokenQty:         test.Instrument.Creation.AuthorizedTokenQty,
		EnforcementOrdersPermitted: true,
		InstrumentType:             test.Instrument.Creation.InstrumentType,
		InstrumentPayload:          currencyBuf.Bytes(),
	}

	amendments, err := test.Instrument.Creation.CreateAmendments(newDefinition)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ := json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	modification := &actions.InstrumentModification{
		InstrumentType:     test.Instrument.Creation.InstrumentType,
		InstrumentCode:     test.Instrument.Creation.InstrumentCode,
		InstrumentRevision: 0,
		Amendments:         amendments,
	}

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint := state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs := []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 150, false, false); err != nil {
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
		OutputIndex: modificationScriptOutputIndex,
		Action:      modification,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     true,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.Broadcaster.GetLastTx()
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

	instrument, err := test.Caches.Caches.Instruments.Get(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode)
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

	time.Sleep(time.Millisecond) // wait for statistics to process

	stats, err := statistics.FetchContractValue(ctx, test.Caches.Cache,
		state.CalculateContractHash(test.ContractLockingScript), uint64(time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("Failed to fetch contract statistics : %s", err)
	}

	js, _ = json.MarshalIndent(stats, "", "  ")
	t.Logf("Stats : %s", js)

	stats.Lock()

	statAction := stats.GetAction(actions.CodeInstrumentModification)
	if statAction == nil {
		t.Fatalf("Missing statistics action for code")
	}

	if statAction.Count != 1 {
		t.Fatalf("Wrong statistics action count : got %d, want %d", statAction.Count, 1)
	}

	if statAction.RejectedCount != 1 {
		t.Fatalf("Wrong statistics action rejection count : got %d, want %d",
			statAction.RejectedCount, 1)
	}

	stats.Unlock()

	StopTestAgent(ctx, t, test)
}
