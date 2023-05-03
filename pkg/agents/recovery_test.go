package agents

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
)

func Test_Recovery_AcceptContractOffer(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

	_, _, masterAddress := state.MockKey()

	config := agent.Config()
	config.RecoveryMode = true
	agent.SetConfig(config)

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

	contractOffer := &actions.ContractOffer{
		ContractName: "Test Contract Name",
		ContractType: actions.ContractTypeEntity,
		Issuer: &actions.EntityField{
			Name: "John Bitcoin",
		},
		MasterAddress: masterAddress.Bytes(),
		ContractFee:   100,
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
		OutputIndex: contractOfferScriptOutputIndex,
		Action:      contractOffer,
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
	if responseTx != nil {
		t.Fatalf("There should not be a response tx in recovery mode")
	}

	recoveryTxs, err := test.caches.Caches.RecoveryTransactions.Get(ctx, test.contractLockingScript)
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
	test.caches.Caches.RecoveryTransactions.Release(ctx, test.contractLockingScript)

	config = agent.Config()
	config.RecoveryMode = false
	agent.SetConfig(config)
	recoverInterrupt := make(chan interface{})
	if err := agent.ProcessRecoveryRequests(ctx, recoverInterrupt); err != nil {
		t.Fatalf("Failed to process recovery requests : %s", err)
	}

	responseTx = test.broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}
	responseTxID := *responseTx.Tx.TxHash()

	t.Logf("Response Tx : %s", responseTx)

	if !responseTx.Tx.TxOut[0].LockingScript.Equal(test.contractLockingScript) {
		t.Errorf("Wrong contract output locking script : got %s, want %s",
			responseTx.Tx.TxOut[0].LockingScript, test.contractLockingScript)
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

		if a, ok := action.(*actions.Rejection); ok {
			js, _ := json.MarshalIndent(a, "", "  ")
			t.Errorf("Rejection : %s", js)
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	test.contract.Lock()
	if test.contract.Formation == nil {
		t.Errorf("Missing state contract formation")
	} else if !test.contract.Formation.Equal(formation) {
		t.Errorf("State contract formation doesn't equal tx action")
	}

	if test.contract.FormationTxID == nil {
		t.Errorf("Missing state contract formation txid")
	} else if !test.contract.FormationTxID.Equal(&responseTxID) {
		t.Errorf("Wrong state contract formation txid : got %s, want %s", test.contract.FormationTxID,
			responseTxID)
	}
	test.contract.Unlock()

	StopTestAgent(ctx, t, test)
}

func Test_Recovery_ContractOfferAlreadyAccepted(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

	_, _, masterAddress := state.MockKey()

	config := agent.Config()
	config.RecoveryMode = true
	agent.SetConfig(config)

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
	if err := tx.AddOutput(test.contractLockingScript, 200, false, false); err != nil {
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
		OutputIndex: contractOfferScriptOutputIndex,
		Action:      contractOffer,
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
	if responseTx != nil {
		t.Fatalf("There should not be a response tx in recovery mode")
	}

	recoveryTxs, err := test.caches.Caches.RecoveryTransactions.Get(ctx, test.contractLockingScript)
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
	test.caches.Caches.RecoveryTransactions.Release(ctx, test.contractLockingScript)

	formationTx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add input
	outpoint = &wire.OutPoint{
		Hash:  *tx.MsgTx.TxHash(),
		Index: 0,
	}
	spentOutputs = []*expanded_tx.Output{
		{
			LockingScript: test.contractLockingScript,
			Value:         tx.MsgTx.TxOut[0].Value,
		},
	}

	if err := formationTx.AddInput(*outpoint, test.contractLockingScript,
		tx.MsgTx.TxOut[0].Value); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := formationTx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	contractFormation, err := contractOffer.Formation()
	if err != nil {
		t.Fatalf("Failed to create formation : %s", err)
	}
	adminAddress, _ := bitcoin.RawAddressFromLockingScript(test.adminLockingScript)
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

	formationTx.SetChangeLockingScript(test.feeLockingScript, "")

	if _, err := formationTx.Sign([]bitcoin.Key{test.contractKey}); err != nil {
		t.Fatalf("Failed to sign tx : %s", err)
	}
	responseTxID := *formationTx.MsgTx.TxHash()

	t.Logf("Created response tx : %s", formationTx.String(bitcoin.MainNet))

	addTransaction = &transactions.Transaction{
		Tx:           formationTx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err = test.caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: contractFormationScriptOutputIndex,
		Action:      contractFormation,
		Agents: []ActionAgent{
			{
				LockingScript: test.contractLockingScript,
				IsRequest:     false,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	recoveryTxs, err = test.caches.Caches.RecoveryTransactions.Get(ctx, test.contractLockingScript)
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
	test.caches.Caches.RecoveryTransactions.Release(ctx, test.contractLockingScript)

	config = agent.Config()
	config.RecoveryMode = false
	agent.SetConfig(config)
	recoverInterrupt := make(chan interface{})
	if err := agent.ProcessRecoveryRequests(ctx, recoverInterrupt); err != nil {
		t.Fatalf("Failed to process recovery requests : %s", err)
	}

	responseTx = test.broadcaster.GetLastTx()
	if responseTx != nil {
		t.Fatalf("There should be not response transactions since the response was already seen")
	}

	test.contract.Lock()
	if test.contract.Formation == nil {
		t.Errorf("Missing state contract formation")
	} else if !test.contract.Formation.Equal(contractFormation) {
		t.Errorf("State contract formation doesn't equal tx action")
	}

	if test.contract.FormationTxID == nil {
		t.Errorf("Missing state contract formation txid")
	} else if !test.contract.FormationTxID.Equal(&responseTxID) {
		t.Errorf("Wrong state contract formation txid : got %s, want %s", test.contract.FormationTxID,
			responseTxID)
	}
	test.contract.Unlock()

	StopTestAgent(ctx, t, test)
}
