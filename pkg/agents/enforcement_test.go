package agents

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
)

func Test_Freeze_Balances_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	balances := state.MockBalances(ctx, &test.Caches.TestCaches, test.Contract, test.Instrument,
		500)

	var freezeBalances []*state.MockBalanceData
	var freezeQuantities []uint64
	balanceIndex := 0
	for i := 0; i < 100; i++ {
		iter := rand.Intn(5)
		if i != 0 {
			iter++
		}

		balanceIndex += iter
		if balanceIndex >= len(balances) {
			break
		}

		freezeBalances = append(freezeBalances, balances[balanceIndex])
		freezeQuantities = append(freezeQuantities,
			uint64(rand.Intn(int(balances[balanceIndex].Quantity))+1))
	}

	t.Logf("Freezing %d balances", len(freezeBalances))

	freezeOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionFreeze,
		InstrumentType:   string(test.Instrument.InstrumentType[:]),
		InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		// FreezeTxId               []byte
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	for i, freezeBalance := range freezeBalances {
		ra, err := bitcoin.RawAddressFromLockingScript(freezeBalance.LockingScript)
		if err != nil {
			t.Fatalf("Failed to create raw address : %s", err)
		}

		freezeOrder.TargetAddresses = append(freezeOrder.TargetAddresses,
			&actions.TargetAddressField{
				Address:  ra.Bytes(),
				Quantity: freezeQuantities[i],
			})
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
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

	freezeOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(freezeOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add freeze order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
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

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: freezeOrderScriptOutputIndex,
		Action:      freezeOrder,
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
	var freeze *actions.Freeze
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Freeze); ok {
			freeze = a
			break
		}
	}

	if freeze == nil {
		t.Fatalf("Missing freeze action")
	}

	js, _ := json.MarshalIndent(freeze, "", "  ")
	t.Logf("Freeze : %s", js)

	// Check freeze action
	for i, target := range freeze.Quantities {
		if int(target.Index) >= len(responseTx.Tx.TxOut) {
			t.Fatalf("Invalid output index %d : %d >= %d", i, target.Index,
				len(responseTx.Tx.TxOut))
		}

		lockingScript := responseTx.Tx.TxOut[target.Index].LockingScript
		if !lockingScript.Equal(freezeBalances[i].LockingScript) {
			t.Errorf("Wrong freeze locking script : got %s, want %s", lockingScript,
				freezeBalances[i].LockingScript)
		}

		totalQuantity := freezeBalances[i].Quantity
		availableQuantity := totalQuantity
		if freezeQuantities[i] > availableQuantity {
			availableQuantity = 0
		} else {
			availableQuantity -= freezeQuantities[i]
		}

		t.Logf("total %d, frozen %d, available %d", totalQuantity, freezeQuantities[i],
			availableQuantity)

		if target.Quantity != availableQuantity {
			t.Errorf("Wrong freeze quantity : got %d, want %d", target.Quantity, availableQuantity)
		}
	}

	// Check that balances are frozen in storage
	lockingScripts := make([]bitcoin.Script, len(freezeBalances))
	for i, freezeBalance := range freezeBalances {
		lockingScripts[i] = freezeBalance.LockingScript
	}

	gotBalances, err := test.Caches.Caches.Balances.GetMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, lockingScripts)
	if err != nil {
		t.Fatalf("Failed to get balances : %s", err)
	}

	for i, gotBalance := range gotBalances {
		freezeQuantity := gotBalance.FrozenQuantity(now)

		if freezeQuantity != freezeQuantities[i] {
			t.Errorf("Wrong freeze quantity %d : got %d, want %d", i, freezeQuantity,
				freezeQuantities[i])
		}

		totalQuantity := freezeBalances[i].Quantity
		availableQuantity := totalQuantity
		if freezeQuantities[i] > availableQuantity {
			availableQuantity = 0
		} else {
			availableQuantity -= freezeQuantities[i]
		}

		gotAvailableQuantity := gotBalance.Available(now)
		if gotAvailableQuantity != availableQuantity {
			t.Errorf("Wrong available quantity %d : got %d, want %d", i, gotAvailableQuantity,
				availableQuantity)
		}
	}

	test.Caches.Caches.Balances.ReleaseMulti(ctx, test.ContractLockingScript, test.Instrument.InstrumentCode,
		gotBalances)

	freezeTxID := *responseTx.Tx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(test.Instrument.InstrumentType[:]),
		// InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		FreezeTxId: freezeTxID[:],
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	tx = txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint = state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

	thawOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(thawOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add thaw order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ = state.MockKey()
	fundingOutpoint = state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ = state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign([]bitcoin.Key{test.AdminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: thawOrderScriptOutputIndex,
		Action:      thawOrder,
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

	responseTx = test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Thaw); ok {
			thaw = a
			break
		}
	}

	if thaw == nil {
		t.Fatalf("Missing thaw action")
	}

	js, _ = json.MarshalIndent(thaw, "", "  ")
	t.Logf("Thaw : %s", js)

	gotBalances, err = test.Caches.Caches.Balances.GetMulti(ctx, test.ContractLockingScript,
		test.Instrument.InstrumentCode, lockingScripts)
	if err != nil {
		t.Fatalf("Failed to get balances : %s", err)
	}

	for i, gotBalance := range gotBalances {
		freezeQuantity := gotBalance.FrozenQuantity(now)

		if freezeQuantity != 0 {
			t.Errorf("Wrong freeze quantity %d : got %d, want %d", i, freezeQuantity, 0)
		}

		totalQuantity := freezeBalances[i].Quantity
		gotAvailableQuantity := gotBalance.Available(now)
		if gotAvailableQuantity != totalQuantity {
			t.Errorf("Wrong available quantity %d : got %d, want %d", i, gotAvailableQuantity,
				totalQuantity)
		}
	}

	test.Caches.Caches.Balances.ReleaseMulti(ctx, test.ContractLockingScript, test.Instrument.InstrumentCode,
		gotBalances)

	StopTestAgent(ctx, t, test)
}

func Test_Freeze_Contract_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to generate contract address : %s", err)
	}

	freezeOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionFreeze,
		// InstrumentType:   string(test.Instrument.InstrumentType[:]),
		// InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		// FreezeTxId               []byte
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	freezeOrder.TargetAddresses = append(freezeOrder.TargetAddresses,
		&actions.TargetAddressField{
			Address:  contractAddress.Bytes(),
			Quantity: 0,
		})

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
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

	freezeOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(freezeOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add freeze order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
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

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: freezeOrderScriptOutputIndex,
		Action:      freezeOrder,
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
	var freeze *actions.Freeze
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Freeze); ok {
			freeze = a
			break
		}
	}

	if freeze == nil {
		t.Fatalf("Missing freeze action")
	}

	js, _ := json.MarshalIndent(freeze, "", "  ")
	t.Logf("Freeze : %s", js)

	if len(freeze.Quantities) != 1 {
		t.Errorf("Wrong freeze quantity count : got %d, want %d", len(freeze.Quantities), 1)
	}

	contractTarget := freeze.Quantities[0]

	if int(contractTarget.Index) >= len(responseTx.Tx.TxOut) {
		t.Fatalf("Invalid output index : %d >= %d", contractTarget.Index, len(responseTx.Tx.TxOut))
	}

	lockingScript := responseTx.Tx.TxOut[contractTarget.Quantity].LockingScript
	if !lockingScript.Equal(test.ContractLockingScript) {
		t.Errorf("Wrong freeze locking script : got %s, want %s", lockingScript,
			test.ContractLockingScript)
	}

	test.Contract.Lock()
	isFrozen := test.Contract.IsFrozen(now)
	test.Contract.Unlock()

	if !isFrozen {
		t.Errorf("Contract should be frozen")
	} else {
		t.Logf("Contract is frozen")
	}

	freezeTxID := *responseTx.Tx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(test.Instrument.InstrumentType[:]),
		// InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		FreezeTxId: freezeTxID[:],
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	t.Logf("Creating Thaw Order")

	tx = txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint = state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

	thawOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(thawOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add thaw order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ = state.MockKey()
	fundingOutpoint = state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ = state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign([]bitcoin.Key{test.AdminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: thawOrderScriptOutputIndex,
		Action:      thawOrder,
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

	responseTx = test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Thaw); ok {
			thaw = a
			break
		}
	}

	if thaw == nil {
		t.Fatalf("Missing thaw action")
	}

	js, _ = json.MarshalIndent(thaw, "", "  ")
	t.Logf("Thaw : %s", js)

	test.Contract.Lock()
	isFrozen = test.Contract.IsFrozen(now)
	test.Contract.Unlock()

	if isFrozen {
		t.Errorf("Contract should not be frozen")
	} else {
		t.Logf("Contract is not frozen")
	}

	StopTestAgent(ctx, t, test)
}

func Test_Freeze_Instrument_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithInstrument(ctx, t)

	contractAddress, err := bitcoin.RawAddressFromLockingScript(test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to generate contract address : %s", err)
	}

	freezeOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionFreeze,
		InstrumentType:   string(test.Instrument.InstrumentType[:]),
		InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		// FreezeTxId               []byte
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	freezeOrder.TargetAddresses = append(freezeOrder.TargetAddresses,
		&actions.TargetAddressField{
			Address:  contractAddress.Bytes(),
			Quantity: 0,
		})

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
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

	freezeOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(freezeOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add freeze order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ := state.MockKey()
	fundingOutpoint := state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
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

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: freezeOrderScriptOutputIndex,
		Action:      freezeOrder,
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
	var freeze *actions.Freeze
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Freeze); ok {
			freeze = a
			break
		}
	}

	if freeze == nil {
		t.Fatalf("Missing freeze action")
	}

	js, _ := json.MarshalIndent(freeze, "", "  ")
	t.Logf("Freeze : %s", js)

	if len(freeze.Quantities) != 1 {
		t.Errorf("Wrong freeze quantity count : got %d, want %d", len(freeze.Quantities), 1)
	}

	contractTarget := freeze.Quantities[0]

	if int(contractTarget.Index) >= len(responseTx.Tx.TxOut) {
		t.Fatalf("Invalid output index : %d >= %d", contractTarget.Index, len(responseTx.Tx.TxOut))
	}

	lockingScript := responseTx.Tx.TxOut[contractTarget.Quantity].LockingScript
	if !lockingScript.Equal(test.ContractLockingScript) {
		t.Errorf("Wrong freeze locking script : got %s, want %s", lockingScript,
			test.ContractLockingScript)
	}

	test.Instrument.Lock()
	isFrozen := test.Instrument.IsFrozen(now)
	test.Instrument.Unlock()

	if !isFrozen {
		t.Errorf("Instrument should be frozen")
	} else {
		t.Logf("Instrument is frozen")
	}

	freezeTxID := *responseTx.Tx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(test.Instrument.InstrumentType[:]),
		// InstrumentCode:   test.Instrument.InstrumentCode[:],
		// TargetAddresses          []*TargetAddressField
		FreezeTxId: freezeTxID[:],
		// FreezePeriod             uint64
		// DepositAddress           []byte
		// AuthorityName            string
		// AuthorityPublicKey       []byte
		// SignatureAlgorithm       uint32
		// OrderSignature           []byte
		// BitcoinDispersions       []*QuantityIndexField
		Message: "Court Order",
		// SupportingEvidenceFormat uint32
		// SupportingEvidence       []byte
		// ReferenceTransactions    []*ReferenceTransactionField
	}

	t.Logf("Creating Thaw Order")

	tx = txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint = state.MockOutPoint(test.AdminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: test.AdminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, test.AdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.ContractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

	thawOrderScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(thawOrderScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add thaw order action output : %s", err)
	}

	// Add funding
	fundingKey, fundingLockingScript, _ = state.MockKey()
	fundingOutpoint = state.MockOutPoint(fundingLockingScript, 700)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: fundingLockingScript,
		Value:         700,
	})

	if err := tx.AddInput(*fundingOutpoint, fundingLockingScript, 700); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	_, changeLockingScript, _ = state.MockKey()
	tx.SetChangeLockingScript(changeLockingScript, "")

	if _, err := tx.Sign([]bitcoin.Key{test.AdminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &transactions.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = test.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: thawOrderScriptOutputIndex,
		Action:      thawOrder,
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

	responseTx = test.Broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.Thaw); ok {
			thaw = a
			break
		}
	}

	if thaw == nil {
		t.Fatalf("Missing thaw action")
	}

	js, _ = json.MarshalIndent(thaw, "", "  ")
	t.Logf("Thaw : %s", js)

	test.Instrument.Lock()
	isFrozen = test.Instrument.IsFrozen(now)
	test.Instrument.Unlock()

	if isFrozen {
		t.Errorf("Instrument should not be frozen")
	} else {
		t.Logf("Instrument is not frozen")
	}

	StopTestAgent(ctx, t, test)
}
