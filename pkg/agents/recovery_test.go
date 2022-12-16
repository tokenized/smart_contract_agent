package agents

import (
	"context"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/peer_channels"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/locker"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Recovery_AcceptContractOffer(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	agentConfig := DefaultConfig()
	agentConfig.RecoveryMode = true
	cacherConfig := cacher.DefaultConfig()

	caches := StartTestCaches(ctx, t, store, cacherConfig, time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, _ := state.MockKey()
	_, _, masterAddress := state.MockKey()
	_, feeLockingScript, _ := state.MockKey()
	adminKey, adminLockingScript, _ := state.MockKey()

	contract, err := caches.Caches.Contracts.Add(ctx, &state.Contract{
		LockingScript: contractLockingScript,
	})
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, agentConfig, feeLockingScript,
		caches.Caches, caches.Transactions, caches.Services, locker, store, broadcaster, nil, nil, nil, nil, peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
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

	contractOffer := &actions.ContractOffer{
		ContractName: "Test Contract Name",
		ContractType: actions.ContractTypeEntity,
		Issuer: &actions.EntityField{
			Name: "John Bitcoin",
		},
		MasterAddress: masterAddress.Bytes(),
	}

	// Add action output
	contractOfferScript, err := protocol.Serialize(contractOffer, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractOfferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(contractOfferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add contract offer action output : %s", err)
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
		OutputIndex:         contractOfferScriptOutputIndex,
		Action:              contractOffer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx != nil {
		t.Fatalf("There should not be a response tx in recovery mode")
	}

	recoveryTxs, err := caches.Caches.RecoveryTransactions.Get(ctx, contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get recovery transactions : %s", err)
	}

	if recoveryTxs == nil {
		t.Fatalf("Recovery transactions not found")
	}

	if len(recoveryTxs.Transactions) != 1 {
		t.Fatalf("Wrong recovery transactions count : got %d, want %d",
			len(recoveryTxs.Transactions), 1)
	}
	caches.Caches.RecoveryTransactions.Release(ctx, contractLockingScript)

	agent.config.RecoveryMode = false
	if err := agent.ProcessRecoveryRequests(ctx); err != nil {
		t.Fatalf("Failed to process recovery requests : %s", err)
	}

	responseTx = broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}
	responseTxID := *responseTx.Tx.TxHash()

	t.Logf("Response Tx : %s", responseTx)

	if !responseTx.Tx.TxOut[0].LockingScript.Equal(contractLockingScript) {
		t.Errorf("Wrong contract output locking script : got %s, want %s",
			responseTx.Tx.TxOut[0].LockingScript, contractLockingScript)
	}

	// Find formation action
	var formation *actions.ContractFormation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.ContractFormation); ok {
			formation = a
			break
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	contract.Lock()
	if contract.Formation == nil {
		t.Errorf("Missing state contract formation")
	} else if !contract.Formation.Equal(formation) {
		t.Errorf("State contract formation doesn't equal tx action")
	}

	if contract.FormationTxID == nil {
		t.Errorf("Missing state contract formation txid")
	} else if !contract.FormationTxID.Equal(&responseTxID) {
		t.Errorf("Wrong state contract formation txid : got %s, want %s", contract.FormationTxID,
			responseTxID)
	}
	contract.Unlock()

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_Recovery_ContractOfferAlreadyAccepted(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()
	agentConfig := DefaultConfig()
	agentConfig.RecoveryMode = true
	cacherConfig := cacher.DefaultConfig()

	caches := StartTestCaches(ctx, t, store, cacherConfig, time.Second)
	locker := locker.NewInlineLocker()

	contractKey, contractLockingScript, _ := state.MockKey()
	_, _, masterAddress := state.MockKey()
	_, feeLockingScript, _ := state.MockKey()
	adminKey, adminLockingScript, adminAddress := state.MockKey()

	contract, err := caches.Caches.Contracts.Add(ctx, &state.Contract{
		LockingScript: contractLockingScript,
	})
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, agentConfig, feeLockingScript,
		caches.Caches, caches.Transactions, caches.Services, locker, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
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
	if err := tx.AddOutput(contractLockingScript, 200, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	contractOffer := &actions.ContractOffer{
		ContractName: "Test Contract Name",
		ContractType: actions.ContractTypeEntity,
		Issuer: &actions.EntityField{
			Name: "John Bitcoin",
		},
		MasterAddress: masterAddress.Bytes(),
	}

	// Add action output
	contractOfferScript, err := protocol.Serialize(contractOffer, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractOfferScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(contractOfferScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add contract offer action output : %s", err)
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
		OutputIndex:         contractOfferScriptOutputIndex,
		Action:              contractOffer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx != nil {
		t.Fatalf("There should not be a response tx in recovery mode")
	}

	recoveryTxs, err := caches.Caches.RecoveryTransactions.Get(ctx, contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get recovery transactions : %s", err)
	}

	if recoveryTxs == nil {
		t.Fatalf("Recovery transactions not found")
	}

	if len(recoveryTxs.Transactions) != 1 {
		t.Fatalf("Wrong recovery transactions count : got %d, want %d",
			len(recoveryTxs.Transactions), 1)
	}
	caches.Caches.RecoveryTransactions.Release(ctx, contractLockingScript)

	formationTx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint = &wire.OutPoint{
		Hash:  *tx.MsgTx.TxHash(),
		Index: 0,
	}
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: contractLockingScript,
			Value:         tx.MsgTx.TxOut[0].Value,
		},
	}

	if err := formationTx.AddInput(*outpoint, contractLockingScript,
		tx.MsgTx.TxOut[0].Value); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := formationTx.AddOutput(contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	contractFormation, err := contractOffer.Formation()
	if err != nil {
		t.Fatalf("Failed to create formation : %s", err)
	}
	contractFormation.AdminAddress = adminAddress.Bytes()
	contractFormation.Timestamp = uint64(time.Now().UnixNano())

	// Add action output
	contractFormationScript, err := protocol.Serialize(contractFormation, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractFormationScriptOutputIndex := len(formationTx.Outputs)
	if err := formationTx.AddOutput(contractFormationScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add contract offer action output : %s", err)
	}

	formationTx.SetChangeLockingScript(feeLockingScript, "")

	if _, err := formationTx.Sign([]bitcoin.Key{contractKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}
	responseTxID := *formationTx.MsgTx.TxHash()

	t.Logf("Created response tx : %s", formationTx.String(bitcoin.MainNet))

	addTransaction = &transactions.Transaction{
		Tx:           formationTx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		AgentLockingScripts: []bitcoin.Script{contractLockingScript},
		OutputIndex:         contractFormationScriptOutputIndex,
		Action:              contractFormation,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Transactions.Release(ctx, transaction.GetTxID())

	recoveryTxs, err = caches.Caches.RecoveryTransactions.Get(ctx, contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get recovery transactions : %s", err)
	}

	if recoveryTxs == nil {
		t.Fatalf("Recovery transactions not found")
	}

	if len(recoveryTxs.Transactions) != 0 {
		t.Fatalf("Wrong recovery transactions count : got %d, want %d",
			len(recoveryTxs.Transactions), 0)
	}
	caches.Caches.RecoveryTransactions.Release(ctx, contractLockingScript)

	agent.config.RecoveryMode = false
	if err := agent.ProcessRecoveryRequests(ctx); err != nil {
		t.Fatalf("Failed to process recovery requests : %s", err)
	}

	responseTx = broadcaster.GetLastTx()
	if responseTx != nil {
		t.Fatalf("There should be not response transactions since the response was already seen")
	}

	contract.Lock()
	if contract.Formation == nil {
		t.Errorf("Missing state contract formation")
	} else if !contract.Formation.Equal(contractFormation) {
		t.Errorf("State contract formation doesn't equal tx action")
	}

	if contract.FormationTxID == nil {
		t.Errorf("Missing state contract formation txid")
	} else if !contract.FormationTxID.Equal(&responseTxID) {
		t.Errorf("Wrong state contract formation txid : got %s, want %s", contract.FormationTxID,
			responseTxID)
	}
	contract.Unlock()

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
