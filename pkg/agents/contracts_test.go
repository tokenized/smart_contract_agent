package agents

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/permissions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Contracts_Offer_Invalid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

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
		ContractType:   actions.ContractTypeInstrument,
		EntityContract: []byte{1, 2},
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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractOfferScriptOutputIndex,
		Action:              contractOffer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

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

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	if rejection.RejectionCode != actions.RejectionsMsgMalformed {
		t.Fatalf("Wrong response rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsMsgMalformed)
	}

	js, _ := json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	StopTestAgent(ctx, t, test)
}

func Test_Contracts_Offer_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

	_, _, masterAddress := state.MockKey()

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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractOfferScriptOutputIndex,
		Action:              contractOffer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}
	responseTxID := *responseTx.Tx.TxHash()

	t.Logf("Response Tx : %s", responseTx)

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

	js, _ := json.MarshalIndent(formation, "", "  ")
	t.Logf("ContractFormation : %s", js)

	if !responseTx.Tx.TxOut[0].LockingScript.Equal(test.contractLockingScript) {
		t.Errorf("Wrong contract output locking script : got %s, want %s",
			responseTx.Tx.TxOut[0].LockingScript, test.contractLockingScript)
	}

	if formation.ContractName != "Test Contract Name" {
		t.Errorf("Wrong formation name : got \"%s\", want \"%s\"", formation.ContractName,
			"Test Contract Name")
	}

	if formation.Issuer == nil {
		t.Errorf("Missing formation issuer")
	} else if formation.Issuer.Name != "John Bitcoin" {
		t.Errorf("Wrong formation issuer name : got \"%s\", want \"%s\"", formation.Issuer.Name,
			"John Bitcoin")
	}

	if formation.ContractType != actions.ContractTypeEntity {
		t.Errorf("Wrong formation contract type : got %d, want %d", formation.ContractType,
			actions.ContractTypeEntity)
	}

	if !bytes.Equal(formation.MasterAddress, masterAddress.Bytes()) {
		t.Errorf("Wrong formation master address : got %x, want %x", formation.MasterAddress,
			masterAddress.Bytes())
	}

	adminAddress, _ := bitcoin.RawAddressFromLockingScript(test.adminLockingScript)
	if !bytes.Equal(formation.AdminAddress, adminAddress.Bytes()) {
		t.Errorf("Wrong formation test.admin address : got %x, want %x", formation.AdminAddress,
			adminAddress.Bytes())
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

func Test_Contracts_Offer_AlreadyExists(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

	var formationTxID bitcoin.Hash32
	rand.Read(formationTxID[:])

	test.contract.Formation = &actions.ContractFormation{
		ContractName: "Existing Contract Name",
	}
	test.contract.FormationTxID = &formationTxID

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
		ContractType: actions.ContractTypeEntity,
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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractOfferScriptOutputIndex,
		Action:              contractOffer,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

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

	rejectData := actions.RejectionsData(rejection.RejectionCode)
	if rejectData != nil {
		t.Logf("Rejection Code : %s", rejectData.Label)
	}

	if rejection.RejectionCode != actions.RejectionsContractExists {
		t.Fatalf("Wrong response rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsContractExists)
	}

	js, _ := json.MarshalIndent(rejection, "", "  ")
	t.Logf("Rejection : %s", js)

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	StopTestAgent(ctx, t, test)
}

func Test_Contracts_Amendment_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

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

	js, _ := json.MarshalIndent(test.contract.Formation, "", "  ")
	t.Logf("Original ContractFormation : %s", js)

	newOffer := &actions.ContractOffer{
		ContractName:           "New Test Name",
		ContractFee:            test.contract.Formation.ContractFee,
		ContractType:           test.contract.Formation.ContractType,
		AdministrationProposal: test.contract.Formation.AdministrationProposal,
		HolderProposal:         test.contract.Formation.HolderProposal,
		EntityContract:         test.contract.Formation.EntityContract,
	}

	amendments, err := test.contract.Formation.CreateAmendments(newOffer)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	js, _ = json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	for _, amendment := range amendments {
		path, err := permissions.FieldIndexPathFromBytes(amendment.FieldIndexPath)
		if err != nil {
			t.Fatalf("Invalid field index path : %s", err)
		}

		t.Logf("Field Index Path : %v", path)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	contractAmendment := &actions.ContractAmendment{
		// ChangeAdministrationAddress bool
		// ChangeOperatorAddress       bool
		// RefTxID                     []byte
		ContractRevision: 0,
		Amendments:       amendments,
	}

	// Add action output
	contractAmendmentScript, err := protocol.Serialize(contractAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(contractAmendmentScript, 0, false, false); err != nil {
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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractAmendmentScriptOutputIndex,
		Action:              contractAmendment,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.broadcaster.GetLastTx()
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
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("Amended ContractFormation : %s", js)

	if formation.ContractName != "New Test Name" {
		t.Errorf("Wrong formation name : got \"%s\", want \"%s\"", formation.ContractName,
			"New Test Name")
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

func Test_Contracts_Amendment_AdminChange(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgentWithContract(ctx, t)

	newAdminKey, newAdminLockingScript, newAdminAddress := state.MockKey()

	tx := txbuilder.NewTxBuilder(0.05, 0.0)

	// Add test.admin input
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

	// Add new test.admin input
	outpoint2 := state.MockOutPoint(newAdminLockingScript, 1)
	spentOutputs = append(spentOutputs, &expanded_tx.Output{
		LockingScript: newAdminLockingScript,
		Value:         1,
	})

	if err := tx.AddInput(*outpoint2, newAdminLockingScript, 1); err != nil {
		t.Fatalf("Failed to add input : %s", err)
	}

	// Add contract output
	if err := tx.AddOutput(test.contractLockingScript, 150, false, false); err != nil {
		t.Fatalf("Failed to add contract output : %s", err)
	}

	js, _ := json.MarshalIndent(test.contract.Formation, "", "  ")
	t.Logf("Original ContractFormation : %s", js)

	contractAmendment := &actions.ContractAmendment{
		ChangeAdministrationAddress: true,
		// ChangeOperatorAddress       bool
		// RefTxID                     []byte
		ContractRevision: 0,
		Amendments:       nil,
	}

	// Add action output
	contractAmendmentScript, err := protocol.Serialize(contractAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(contractAmendmentScript, 0, false, false); err != nil {
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

	if _, err := tx.Sign([]bitcoin.Key{test.adminKey, newAdminKey, fundingKey}); err != nil {
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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractAmendmentScriptOutputIndex,
		Action:              contractAmendment,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.broadcaster.GetLastTx()
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
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("Amended ContractFormation : %s", js)

	if !bytes.Equal(formation.AdminAddress, newAdminAddress.Bytes()) {
		t.Errorf("Wrong formation test.admin address : got 0x%x, want 0x%x", formation.AdminAddress,
			newAdminAddress.Bytes())
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

func Test_Contracts_Amendment_Proposal(t *testing.T) {
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

	js, _ := json.MarshalIndent(test.contract.Formation, "", "  ")
	t.Logf("Original ContractFormation : %s", js)

	newOffer := &actions.ContractOffer{
		ContractName:           "New Test Name",
		ContractFee:            test.contract.Formation.ContractFee,
		ContractType:           test.contract.Formation.ContractType,
		EntityContract:         test.contract.Formation.EntityContract,
		VotingSystems:          test.contract.Formation.VotingSystems,
		AdministrationProposal: test.contract.Formation.AdministrationProposal,
		HolderProposal:         test.contract.Formation.HolderProposal,
		ContractPermissions:    test.contract.Formation.ContractPermissions,
	}

	amendments, err := test.contract.Formation.CreateAmendments(newOffer)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : got %d, want %d", len(amendments), 1)
	}

	js, _ = json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	vote := MockVoteContractAmendmentCompleted(ctx, test.caches, test.adminLockingScript,
		test.contractLockingScript, 0, amendments)
	vote.Lock()
	voteTxID := *vote.VoteTxID

	js, _ = json.MarshalIndent(vote, "", "  ")
	t.Logf("Vote : %s", js)

	contractAmendment := &actions.ContractAmendment{
		// ChangeAdministrationAddress bool
		// ChangeOperatorAddress       bool
		RefTxID:          vote.ResultTxID[:],
		ContractRevision: 0,
		Amendments:       amendments,
	}
	vote.Unlock()

	// Add action output
	contractAmendmentScript, err := protocol.Serialize(contractAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract offer action : %s", err)
	}

	contractAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(contractAmendmentScript, 0, false, false); err != nil {
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
		AgentLockingScripts: []bitcoin.Script{test.contractLockingScript},
		OutputIndex:         contractAmendmentScriptOutputIndex,
		Action:              contractAmendment,
	}}); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	test.caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := test.broadcaster.GetLastTx()
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
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("Amended ContractFormation : %s", js)

	if formation.ContractName != "New Test Name" {
		t.Errorf("Wrong formation name : got \"%s\", want \"%s\"", formation.ContractName,
			"New Test Name")
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

	test.caches.Caches.Votes.Release(ctx, test.contractLockingScript, voteTxID)

	StopTestAgent(ctx, t, test)
}
