package agents

import (
	"context"
	"crypto/rand"
	"encoding/json"
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
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_BodyOfAgreement_Offer_Valid(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContract(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	contract.Formation.BodyOfAgreementType = actions.ContractBodyOfAgreementTypeFull
	contract.MarkModified()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	offer := &actions.BodyOfAgreementOffer{
		Chapters: []*actions.ChapterField{
			{
				Title:    "Chapter 1",
				Preamble: "This is the first chapter.",
				Articles: []*actions.ClauseField{
					{
						Title: "Clause 1",
						Body:  "This is the first paragraph of chapter 1.",
						Children: []*actions.ClauseField{
							{
								Title: "Clause 1.1",
								Body:  "This is the first paragraph of chapter 1.1. It contains [Term1]()",
							},
						},
					},
				},
			},
		},
		Definitions: []*actions.DefinedTermField{
			{
				Term:       "Term1",
				Definition: "Term1 is the first defined term.",
			},
		},
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
	offerScript, err := protocol.Serialize(offer, true)
	if err != nil {
		t.Fatalf("Failed to serialize body of agreement offer action : %s", err)
	}

	offerScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(offerScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add body of agreement offer action output : %s", err)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: offerScriptOutputIndex,
		Action:      offer,
	}}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find formation action
	var formation *actions.BodyOfAgreementFormation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.BodyOfAgreementFormation); ok {
			formation = a
			break
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ := json.MarshalIndent(formation, "", "  ")
	t.Logf("BodyOfAgreementFormation : %s", js)

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_BodyOfAgreement_Offer_UnreferencedTerm(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContract(ctx,
		caches)
	_, feeLockingScript, _ := state.MockKey()

	contract.Formation.BodyOfAgreementType = actions.ContractBodyOfAgreementTypeFull
	contract.MarkModified()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	offer := &actions.BodyOfAgreementOffer{
		Chapters: []*actions.ChapterField{
			{
				Title:    "Chapter 1",
				Preamble: "This is the first chapter.",
				Articles: []*actions.ClauseField{
					{
						Title: "Clause 1",
						Body:  "This is the first paragraph of chapter 1.",
						Children: []*actions.ClauseField{
							{
								Title: "Clause 1.1",
								Body:  "This is the first paragraph of chapter 1.1",
							},
						},
					},
				},
			},
		},
		Definitions: []*actions.DefinedTermField{
			{
				Term:       "Term1",
				Definition: "Term1 is the first defined term.",
			},
		},
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
	offerScript, err := protocol.Serialize(offer, true)
	if err != nil {
		t.Fatalf("Failed to serialize body of agreement offer action : %s", err)
	}

	offerScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(offerScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add body of agreement offer action output : %s", err)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: offerScriptOutputIndex,
		Action:      offer,
	}}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
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

	if rejection.RejectionCode != actions.RejectionsMsgMalformed {
		t.Errorf("Wrong rejection code : got %d, want %d", rejection.RejectionCode,
			actions.RejectionsMsgMalformed)
	}

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_BodyOfAgreement_Amendment_Basic(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContract(ctx, caches)
	_, feeLockingScript, _ := state.MockKey()

	contract.Lock()
	contract.Formation.BodyOfAgreementType = actions.ContractBodyOfAgreementTypeFull
	contract.BodyOfAgreementFormation = &actions.BodyOfAgreementFormation{
		Chapters: []*actions.ChapterField{
			{
				Title:    "Chapter 1",
				Preamble: "This is the first chapter.",
				Articles: []*actions.ClauseField{
					{
						Title: "Clause 1",
						Body:  "This is the first paragraph of chapter 1.",
						Children: []*actions.ClauseField{
							{
								Title: "Clause 1.1",
								Body:  "This is the first paragraph of chapter 1.1 It contains [Term1]().",
							},
						},
					},
				},
			},
		},
		Definitions: []*actions.DefinedTermField{
			{
				Term:       "Term1",
				Definition: "Term1 is the first defined term.",
			},
		},
	}
	contract.BodyOfAgreementFormationTxID = &bitcoin.Hash32{}
	rand.Read(contract.BodyOfAgreementFormationTxID[:])
	contract.MarkModified()
	contract.Unlock()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	js, _ := json.MarshalIndent(contract.BodyOfAgreementFormation, "", "  ")
	t.Logf("Original BodyOfAgreementFormation : %s", js)

	newOffer := &actions.BodyOfAgreementOffer{
		Chapters:    contract.BodyOfAgreementFormation.Chapters,
		Definitions: contract.BodyOfAgreementFormation.Definitions,
	}

	newOffer.Chapters = append(newOffer.Chapters, &actions.ChapterField{
		Title:    "Appended Chapter",
		Preamble: "Appended preamble.",
	})

	js, _ = json.MarshalIndent(newOffer, "", "  ")
	t.Logf("New BodyOfAgreementOffer : %s", js)

	amendments, err := contract.BodyOfAgreementFormation.CreateAmendments(newOffer)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : %s", err)
	}

	js, _ = json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	bodyAmendment := &actions.BodyOfAgreementAmendment{
		Revision:   0,
		Amendments: amendments,
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
	bodyAmendmentScript, err := protocol.Serialize(bodyAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize body of agreement amendment action : %s", err)
	}

	bodyAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(bodyAmendmentScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add body of agreement amendment action output : %s", err)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: bodyAmendmentScriptOutputIndex,
		Action:      bodyAmendment,
	}}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find formation action
	var formation *actions.BodyOfAgreementFormation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.BodyOfAgreementFormation); ok {
			formation = a
			break
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("BodyOfAgreementFormation : %s", js)

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_BodyOfAgreement_Amendment_Child(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, adminKey, adminLockingScript, contract := state.MockContract(ctx, caches)
	_, feeLockingScript, _ := state.MockKey()

	contract.Lock()
	contract.Formation.BodyOfAgreementType = actions.ContractBodyOfAgreementTypeFull
	contract.BodyOfAgreementFormation = &actions.BodyOfAgreementFormation{
		Chapters: []*actions.ChapterField{
			{
				Title:    "Chapter 1",
				Preamble: "This is the first chapter.",
				Articles: []*actions.ClauseField{
					{
						Title: "Clause 1",
						Body:  "This is the first paragraph of chapter 1.",
						Children: []*actions.ClauseField{
							{
								Title: "Clause 1.1",
								Body:  "This is the first paragraph of chapter 1.1 It contains [Term1]().",
							},
						},
					},
				},
			},
		},
		Definitions: []*actions.DefinedTermField{
			{
				Term:       "Term1",
				Definition: "Term1 is the first defined term.",
			},
		},
	}
	contract.BodyOfAgreementFormationTxID = &bitcoin.Hash32{}
	rand.Read(contract.BodyOfAgreementFormationTxID[:])
	contract.MarkModified()
	contract.Unlock()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	js, _ := json.MarshalIndent(contract.BodyOfAgreementFormation, "", "  ")
	t.Logf("Original BodyOfAgreementFormation : %s", js)

	copyScript, err := protocol.Serialize(contract.BodyOfAgreementFormation, true)
	if err != nil {
		t.Fatalf("Failed to serialize formation : %s", err)
	}

	action, err := protocol.Deserialize(copyScript, true)
	if err != nil {
		t.Fatalf("Failed to deserialize formation : %s", err)
	}

	formationCopy, ok := action.(*actions.BodyOfAgreementFormation)
	if !ok {
		t.Fatalf("Not a body of agreement formation : %s", err)
	}

	newOffer := &actions.BodyOfAgreementOffer{
		Chapters:    formationCopy.Chapters,
		Definitions: formationCopy.Definitions,
	}

	newOffer.Chapters[0].Articles = append(newOffer.Chapters[0].Articles, &actions.ClauseField{
		Title: "Appended Clause",
		Body:  "This is the body of the appended clause.",
	})

	js, _ = json.MarshalIndent(newOffer, "", "  ")
	t.Logf("New BodyOfAgreementOffer : %s", js)

	amendments, err := contract.BodyOfAgreementFormation.CreateAmendments(newOffer)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : %s", err)
	}

	js, _ = json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	bodyAmendment := &actions.BodyOfAgreementAmendment{
		Revision:   0,
		Amendments: amendments,
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
	bodyAmendmentScript, err := protocol.Serialize(bodyAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize body of agreement amendment action : %s", err)
	}

	bodyAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(bodyAmendmentScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add body of agreement amendment action output : %s", err)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: bodyAmendmentScriptOutputIndex,
		Action:      bodyAmendment,
	}}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find formation action
	var formation *actions.BodyOfAgreementFormation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.BodyOfAgreementFormation); ok {
			formation = a
			break
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("BodyOfAgreementFormation : %s", js)

	agent.Release(ctx)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}

func Test_BodyOfAgreement_Amendment_Proposal(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

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
		caches, votingSystems)
	_, feeLockingScript, _ := state.MockKey()

	contract.Lock()
	contract.Formation.BodyOfAgreementType = actions.ContractBodyOfAgreementTypeFull
	contract.BodyOfAgreementFormation = &actions.BodyOfAgreementFormation{
		Chapters: []*actions.ChapterField{
			{
				Title:    "Chapter 1",
				Preamble: "This is the first chapter.",
				Articles: []*actions.ClauseField{
					{
						Title: "Clause 1",
						Body:  "This is the first paragraph of chapter 1.",
						Children: []*actions.ClauseField{
							{
								Title: "Clause 1.1",
								Body:  "This is the first paragraph of chapter 1.1 It contains [Term1]().",
							},
						},
					},
				},
			},
		},
		Definitions: []*actions.DefinedTermField{
			{
				Term:       "Term1",
				Definition: "Term1 is the first defined term.",
			},
		},
	}
	contract.BodyOfAgreementFormationTxID = &bitcoin.Hash32{}
	rand.Read(contract.BodyOfAgreementFormationTxID[:])
	contract.MarkModified()
	contract.Unlock()

	agent, err := NewAgent(ctx, contractKey, contractLockingScript, DefaultConfig(),
		feeLockingScript, caches.Caches, store, broadcaster, nil, nil, nil, nil,
		peer_channels.NewFactory())
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	js, _ := json.MarshalIndent(contract.BodyOfAgreementFormation, "", "  ")
	t.Logf("Original BodyOfAgreementFormation : %s", js)

	newOffer := &actions.BodyOfAgreementOffer{
		Chapters:    contract.BodyOfAgreementFormation.Chapters,
		Definitions: contract.BodyOfAgreementFormation.Definitions,
	}

	newOffer.Chapters = append(newOffer.Chapters, &actions.ChapterField{
		Title:    "Appended Chapter",
		Preamble: "Appended preamble.",
	})

	js, _ = json.MarshalIndent(newOffer, "", "  ")
	t.Logf("New BodyOfAgreementOffer : %s", js)

	amendments, err := contract.BodyOfAgreementFormation.CreateAmendments(newOffer)
	if err != nil {
		t.Fatalf("Failed to create amendments : %s", err)
	}

	if len(amendments) != 1 {
		t.Fatalf("Wrong amendment count : %s", err)
	}

	js, _ = json.MarshalIndent(amendments, "", "  ")
	t.Logf("Amendments : %s", js)

	vote := state.MockVoteContractAmendmentCompleted(ctx, caches, adminLockingScript,
		contractLockingScript, 0, amendments)
	vote.Lock()
	voteTxID := *vote.VoteTxID
	refTxID := *vote.ResultTxID

	js, _ = json.MarshalIndent(vote, "", "  ")
	t.Logf("Vote : %s", js)
	vote.Unlock()

	bodyAmendment := &actions.BodyOfAgreementAmendment{
		RefTxID:    refTxID.Bytes(),
		Revision:   0,
		Amendments: amendments,
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
	bodyAmendmentScript, err := protocol.Serialize(bodyAmendment, true)
	if err != nil {
		t.Fatalf("Failed to serialize body of agreement amendment action : %s", err)
	}

	bodyAmendmentScriptOutputIndex := len(tx.Outputs)
	if err := tx.AddOutput(bodyAmendmentScript, 0, false, false); err != nil {
		t.Fatalf("Failed to add body of agreement amendment action output : %s", err)
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

	addTransaction := &state.Transaction{
		Tx:           tx.MsgTx,
		SpentOutputs: spentOutputs,
	}

	transaction, err := caches.Caches.Transactions.Add(ctx, addTransaction)
	if err != nil {
		t.Fatalf("Failed to add transaction : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, transaction, []Action{{
		OutputIndex: bodyAmendmentScriptOutputIndex,
		Action:      bodyAmendment,
	}}, now); err != nil {
		t.Fatalf("Failed to process transaction : %s", err)
	}

	caches.Caches.Transactions.Release(ctx, transaction.GetTxID())

	responseTx := broadcaster.GetLastTx()
	if responseTx == nil {
		t.Fatalf("No response tx")
	}

	t.Logf("Response Tx : %s", responseTx)

	// Find formation action
	var formation *actions.BodyOfAgreementFormation
	for _, txout := range responseTx.Tx.TxOut {
		action, err := protocol.Deserialize(txout.LockingScript, true)
		if err != nil {
			continue
		}

		if a, ok := action.(*actions.BodyOfAgreementFormation); ok {
			formation = a
			break
		}
	}

	if formation == nil {
		t.Fatalf("Missing formation action")
	}

	js, _ = json.MarshalIndent(formation, "", "  ")
	t.Logf("BodyOfAgreementFormation : %s", js)

	agent.Release(ctx)
	caches.Caches.Votes.Release(ctx, contractLockingScript, voteTxID)
	caches.Caches.Contracts.Release(ctx, contractLockingScript)
	caches.StopTestCaches()
}
