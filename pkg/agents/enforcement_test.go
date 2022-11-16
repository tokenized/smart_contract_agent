package agents

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Freeze_Balances_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	balances := state.MockBalances(ctx, caches, contract, instrument, 500)

	var freezeBalances []*state.MockBalance
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
		InstrumentType:   string(instrument.InstrumentType[:]),
		InstrumentCode:   instrument.InstrumentCode[:],
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
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{freezeOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var freeze *actions.Freeze
	for _, txout := range responseTx.TxOut {
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
		if int(target.Index) >= len(responseTx.TxOut) {
			t.Fatalf("Invalid output index %d : %d >= %d", i, target.Index,
				len(responseTx.TxOut))
		}

		lockingScript := responseTx.TxOut[target.Index].LockingScript
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

	gotBalances, err := caches.Caches.Balances.GetMulti(ctx, contractLockingScript,
		instrument.InstrumentCode, lockingScripts)
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

	caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrument.InstrumentCode,
		gotBalances)

	freezeTxID := *responseTx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(instrument.InstrumentType[:]),
		// InstrumentCode:   instrument.InstrumentCode[:],
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
	outpoint = state.MockOutPoint(adminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{thawOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx = broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.TxOut {
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

	gotBalances, err = caches.Caches.Balances.GetMulti(ctx, contractLockingScript,
		instrument.InstrumentCode, lockingScripts)
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

	caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrument.InstrumentCode,
		gotBalances)

	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Freeze_Contract_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to generate contract address : %s", err)
	}

	freezeOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionFreeze,
		// InstrumentType:   string(instrument.InstrumentType[:]),
		// InstrumentCode:   instrument.InstrumentCode[:],
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
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{freezeOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var freeze *actions.Freeze
	for _, txout := range responseTx.TxOut {
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

	if int(contractTarget.Index) >= len(responseTx.TxOut) {
		t.Fatalf("Invalid output index : %d >= %d", contractTarget.Index, len(responseTx.TxOut))
	}

	lockingScript := responseTx.TxOut[contractTarget.Quantity].LockingScript
	if !lockingScript.Equal(contractLockingScript) {
		t.Errorf("Wrong freeze locking script : got %s, want %s", lockingScript,
			contractLockingScript)
	}

	contract.Lock()
	isFrozen := contract.IsFrozen(now)
	contract.Unlock()

	if !isFrozen {
		t.Errorf("Contract should be frozen")
	} else {
		t.Logf("Contract is frozen")
	}

	freezeTxID := *responseTx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(instrument.InstrumentType[:]),
		// InstrumentCode:   instrument.InstrumentCode[:],
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
	outpoint = state.MockOutPoint(adminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{thawOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx = broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.TxOut {
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

	contract.Lock()
	isFrozen = contract.IsFrozen(now)
	contract.Unlock()

	if isFrozen {
		t.Errorf("Contract should not be frozen")
	} else {
		t.Logf("Contract is not frozen")
	}

	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Freeze_Instrument_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract, instrument := state.MockInstrument(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	instrumentID, _ := protocol.InstrumentIDForRaw(string(instrument.InstrumentType[:]),
		instrument.InstrumentCode[:])
	t.Logf("Mocked instrument : %s", instrumentID)

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	contractAddress, err := bitcoin.RawAddressFromLockingScript(contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to generate contract address : %s", err)
	}

	freezeOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionFreeze,
		InstrumentType:   string(instrument.InstrumentType[:]),
		InstrumentCode:   instrument.InstrumentCode[:],
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
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	freezeOrderScript, err := protocol.Serialize(freezeOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize freeze order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{freezeOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var freeze *actions.Freeze
	for _, txout := range responseTx.TxOut {
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

	if int(contractTarget.Index) >= len(responseTx.TxOut) {
		t.Fatalf("Invalid output index : %d >= %d", contractTarget.Index, len(responseTx.TxOut))
	}

	lockingScript := responseTx.TxOut[contractTarget.Quantity].LockingScript
	if !lockingScript.Equal(contractLockingScript) {
		t.Errorf("Wrong freeze locking script : got %s, want %s", lockingScript,
			contractLockingScript)
	}

	instrument.Lock()
	isFrozen := instrument.IsFrozen(now)
	instrument.Unlock()

	if !isFrozen {
		t.Errorf("Instrument should be frozen")
	} else {
		t.Logf("Instrument is frozen")
	}

	freezeTxID := *responseTx.TxHash()

	thawOrder := &actions.Order{
		ComplianceAction: actions.ComplianceActionThaw,
		// InstrumentType:   string(instrument.InstrumentType[:]),
		// InstrumentCode:   instrument.InstrumentCode[:],
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
	outpoint = state.MockOutPoint(adminLockingScript, 1)
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: adminLockingScript,
			Value:         1,
		},
	}

	if err := tx.AddInput(*outpoint, adminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(contractLockingScript, 500, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	// Add action output
	thawOrderScript, err := protocol.Serialize(thawOrder, true)
	if err != nil {
		t.Fatalf("Failed to serialize thaw order action : %s", err)
	}

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

	if _, err := tx.Sign([]bitcoin.Key{adminKey, fundingKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}

	t.Logf("Created tx : %s", tx.String(bitcoin.MainNet))

	addTransaction = &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []actions.Action{thawOrder}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx = broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find creation action
	var thaw *actions.Thaw
	for _, txout := range responseTx.TxOut {
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

	instrument.Lock()
	isFrozen = instrument.IsFrozen(now)
	instrument.Unlock()

	if isFrozen {
		t.Errorf("Instrument should not be frozen")
	} else {
		t.Logf("Instrument is not frozen")
	}

	caches.Caches.Instruments.Release(ctx, contractLockingScript, instrument.InstrumentCode)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
