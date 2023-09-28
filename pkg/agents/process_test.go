package agents

import (
	"bytes"
	"context"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/smart_contract_agent/pkg/transactions"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
	"github.com/tokenized/txbuilder"
)

func Test_Process(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	agent, test := StartTestAgent(ctx, t)

	// Create a contract by processing contract formation.
	var outputs []*expanded_tx.Output
	tx := wire.NewMsgTx(1)

	// Contract input
	offerTx := wire.NewMsgTx(1)
	offer := &actions.ContractOffer{}
	offerScript, _ := protocol.Serialize(offer, true)
	offerTx.AddTxOut(wire.NewTxOut(0, offerScript))
	offerTx.AddTxOut(wire.NewTxOut(2200, test.ContractLockingScript))
	contractOfferTxID := *offerTx.TxHash()

	contractOfferTransaction := &transactions.Transaction{
		Tx:           offerTx,
		State:        transactions.TxStateSafe,
		SpentOutputs: outputs,
	}

	contractOfferTransaction, err := test.Caches.Transactions.Add(ctx,
		contractOfferTransaction)
	if err != nil {
		t.Fatalf("Failed to add contract offer tx : %s", err)
	}
	test.Caches.Transactions.Release(ctx, contractOfferTxID)

	outputs = append(outputs, &expanded_tx.Output{
		LockingScript: test.ContractLockingScript,
		Value:         2200,
	})
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&contractOfferTxID, 0), make([]byte,
		txbuilder.MaximumP2PKHSigScriptSize)))

	// Contract output
	tx.AddTxOut(wire.NewTxOut(50, test.ContractLockingScript))

	// Contract formation output
	ra, err := test.AdminKey.RawAddress()
	if err != nil {
		t.Fatalf("Failed to create raw address : %s", err)
	}

	_, _, entityAddress := state.MockKey()

	contractFormation := &actions.ContractFormation{
		ContractName:   "Test Contract",
		Timestamp:      uint64(time.Now().UnixNano()),
		ContractType:   actions.ContractTypeInstrument,
		EntityContract: entityAddress.Bytes(),
		AdminAddress:   ra.Bytes(),
	}

	contractFormationScript, err := protocol.Serialize(contractFormation, true)
	if err != nil {
		t.Fatalf("Failed to serialize contract formation : %s", err)
	}

	contractFormationScriptOutputIndex := len(tx.TxOut)
	tx.AddTxOut(wire.NewTxOut(0, contractFormationScript))
	contractFormationTxID := *tx.TxHash()

	contractFormationTransaction := &transactions.Transaction{
		Tx:           tx,
		State:        transactions.TxStateSafe,
		SpentOutputs: outputs,
	}

	contractFormationTransaction, err = test.Caches.Transactions.Add(ctx,
		contractFormationTransaction)
	if err != nil {
		t.Fatalf("Failed to add contract formation tx : %s", err)
	}

	if err := agent.Process(ctx, contractFormationTransaction, []Action{{
		OutputIndex: contractFormationScriptOutputIndex,
		Action:      contractFormation,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     false,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process contract formation : %s", err)
	}
	test.Caches.Transactions.Release(ctx, contractFormationTxID)

	// Check contract is correct.
	currentContract, err := test.Caches.Caches.Contracts.Get(ctx, test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get contract : %s", err)
	}
	if currentContract == nil {
		t.Fatalf("Contract not found")
	}

	currentContract.Lock()

	// Check instrument is correct.
	if currentContract.Formation == nil {
		t.Fatalf("Missing contract formation")
	}

	if !currentContract.Formation.Equal(contractFormation) {
		t.Errorf("Contract formation does not match")
	}

	if currentContract.FormationTxID == nil {
		t.Fatalf("Missing contract formation txid")
	}

	if !currentContract.FormationTxID.Equal(&contractFormationTxID) {
		t.Errorf("Contract formation txid does not match")
	}

	t.Logf("Found %d instruments", currentContract.InstrumentCount)

	currentContract.Unlock()
	test.Caches.Caches.Contracts.Release(ctx, test.ContractLockingScript)

	// Create instrument by processing instrument creation.
	outputs = nil
	tx = wire.NewMsgTx(1)

	// Contract input
	definitionTx := wire.NewMsgTx(1)
	definition := &actions.InstrumentDefinition{}
	definitionScript, _ := protocol.Serialize(definition, true)
	definitionTx.AddTxOut(wire.NewTxOut(0, definitionScript))
	definitionTx.AddTxOut(wire.NewTxOut(2200, test.ContractLockingScript))
	instrumentDefinitionTxID := *definitionTx.TxHash()

	instrumentDefinitionTransaction := &transactions.Transaction{
		Tx:           definitionTx,
		State:        transactions.TxStateSafe,
		SpentOutputs: outputs,
	}

	instrumentDefinitionTransaction, err = test.Caches.Transactions.Add(ctx,
		instrumentDefinitionTransaction)
	if err != nil {
		t.Fatalf("Failed to add instrument definition tx : %s", err)
	}
	test.Caches.Transactions.Release(ctx, instrumentDefinitionTxID)

	outputs = append(outputs, &expanded_tx.Output{
		LockingScript: test.ContractLockingScript,
		Value:         2200,
	})
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&instrumentDefinitionTxID, 0), make([]byte,
		txbuilder.MaximumP2PKHSigScriptSize)))

	// Contract output
	tx.AddTxOut(wire.NewTxOut(50, test.ContractLockingScript))

	// Instrument creation output
	currency := instruments.Currency{
		CurrencyCode: "USD",
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		t.Fatalf("Failed to serialize currency payload : %s", err)
	}

	contractAddress, _ := bitcoin.RawAddressFromLockingScript(test.ContractLockingScript)

	instrumentCode := state.InstrumentCode(protocol.InstrumentCodeFromContract(contractAddress, 0))
	authorizedQuantity := uint64(1000000000)
	instrumentCreation := &actions.InstrumentCreation{
		InstrumentCode:     instrumentCode.Bytes(),
		InstrumentIndex:    0,
		AuthorizedTokenQty: authorizedQuantity,
		InstrumentType:     instruments.CodeCurrency,
		InstrumentPayload:  currencyBuf.Bytes(),
		Timestamp:          uint64(time.Now().UnixNano()),
	}

	t.Logf("Creating instrument : %s", protocol.InstrumentID(currency.Code(),
		bitcoin.Hash20(instrumentCode)))

	instrumentCreationScript, err := protocol.Serialize(instrumentCreation, true)
	if err != nil {
		t.Fatalf("Failed to serialize instrument creation : %s", err)
	}

	instrumentCreationScriptOutputIndex := len(tx.TxOut)
	tx.AddTxOut(wire.NewTxOut(0, instrumentCreationScript))
	instrumentCreationTxID := *tx.TxHash()

	instrumentCreationTx := &transactions.Transaction{
		Tx:           tx,
		State:        transactions.TxStateSafe,
		SpentOutputs: outputs,
	}

	instrumentCreationTx, err = test.Caches.Transactions.Add(ctx, instrumentCreationTx)
	if err != nil {
		t.Fatalf("Failed to add instrument creation tx : %s", err)
	}

	if err := agent.Process(ctx, instrumentCreationTx, []Action{{
		OutputIndex: instrumentCreationScriptOutputIndex,
		Action:      instrumentCreation,
		Agents: []ActionAgent{
			{
				LockingScript: test.ContractLockingScript,
				IsRequest:     false,
			},
		},
	}}); err != nil {
		t.Fatalf("Failed to process instrument creation : %s", err)
	}
	test.Caches.Transactions.Release(ctx, instrumentCreationTxID)

	// Check instrument is correct.
	currentContract, err = test.Caches.Caches.Contracts.Get(ctx, test.ContractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get contract : %s", err)
	}
	if currentContract == nil {
		t.Fatalf("Contract not found")
	}

	currentContract.Lock()

	// Check instrument is correct.
	if currentContract.Formation == nil {
		t.Fatalf("Missing contract formation")
	}

	if !currentContract.Formation.Equal(contractFormation) {
		t.Errorf("Contract formation does not match")
	}

	if currentContract.FormationTxID == nil {
		t.Fatalf("Missing contract formation txid")
	}

	if !currentContract.FormationTxID.Equal(&contractFormationTxID) {
		t.Errorf("Contract formation txid does not match")
	}

	t.Logf("Found %d instruments", currentContract.InstrumentCount)

	for i := uint64(0); i < currentContract.InstrumentCount; i++ {
		instrumentCode := state.InstrumentCode(protocol.InstrumentCodeFromContract(contractAddress,
			i))
		t.Logf("Instrument : %s", protocol.InstrumentID(instruments.CodeCurrency,
			bitcoin.Hash20(instrumentCode)))

		gotInstrument, err := test.Caches.Caches.Instruments.Get(ctx, test.ContractLockingScript,
			instrumentCode)
		if err != nil {
			t.Fatalf("Failed to get instrument : %s", err)
		}

		if gotInstrument == nil {
			t.Fatalf("Instrument missing")
		}
		test.Caches.Caches.Instruments.Release(ctx, test.ContractLockingScript, instrumentCode)

		if !gotInstrument.InstrumentCode.Equal(instrumentCode) {
			t.Errorf("Wrong instrument code : got %s, want %s", gotInstrument.InstrumentCode,
				instrumentCode)
		}

		if gotInstrument.Creation == nil {
			t.Fatalf("Missing instrument creation")
		}

		if !gotInstrument.Creation.Equal(instrumentCreation) {
			t.Errorf("Instrument creation does not match")
		}

		if gotInstrument.CreationTxID == nil {
			t.Fatalf("Missing instrument creation txid")
		}

		if !gotInstrument.CreationTxID.Equal(&instrumentCreationTxID) {
			t.Errorf("Instrument creation txid does not match")
		}
	}

	currentContract.Unlock()
	test.Caches.Caches.Contracts.Release(ctx, test.ContractLockingScript)

	if err := test.Caches.IsFailed(); err != nil {
		t.Fatalf("Cache failed : %s", err)
	}

	// Check test.admin balance is correct.
	adminBalance, err := test.Caches.Caches.Balances.Get(ctx, test.ContractLockingScript, instrumentCode,
		test.AdminLockingScript)
	if err != nil {
		t.Fatalf("Failed to get test.admin balance : %s", err)
	}

	if adminBalance == nil {
		t.Fatalf("Missing test.admin balance")
	}

	adminBalance.Lock()
	t.Logf("Admin balance : %d", adminBalance.Quantity)

	if adminBalance.Quantity != authorizedQuantity {
		t.Errorf("Wrong test.admin balance quantity : got %d, want %d", adminBalance.Quantity,
			authorizedQuantity)
	}
	adminBalance.Unlock()
	test.Caches.Caches.Balances.Release(ctx, test.ContractLockingScript, instrumentCode, adminBalance)

	if err := test.Caches.IsFailed(); err != nil {
		t.Fatalf("Cache failed : %s", err)
	}

	// Transfer to a lot of locking scripts.
	t.Logf("Transfering balances")
	var lockingScripts []bitcoin.Script
	var quantities []uint64
	var txids []bitcoin.Hash32
	remainingQuantity := authorizedQuantity
	recipientCount := 10
	for i := 0; i < recipientCount; i++ {
		if err := test.Caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to generate key : %s", err)
		}

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		quantity := uint64(mathRand.Intn(100000) + 1)

		lockingScripts = append(lockingScripts, lockingScript)
		quantities = append(quantities, quantity)
		remainingQuantity -= quantity

		outputs = nil
		tx = wire.NewMsgTx(1)

		// Contract input
		transferTx := wire.NewMsgTx(1)
		transfer := &actions.Transfer{}
		transferScript, _ := protocol.Serialize(transfer, true)
		transferTx.AddTxOut(wire.NewTxOut(0, transferScript))
		transferTx.AddTxOut(wire.NewTxOut(2200, test.ContractLockingScript))
		transferTxID := *transferTx.TxHash()

		transferTransaction := &transactions.Transaction{
			Tx:           transferTx,
			State:        transactions.TxStateSafe,
			SpentOutputs: outputs,
		}

		transferTransaction, err = test.Caches.Transactions.Add(ctx,
			transferTransaction)
		if err != nil {
			t.Fatalf("Failed to add transfer tx : %s", err)
		}
		test.Caches.Transactions.Release(ctx, transferTxID)

		outputs = append(outputs, &expanded_tx.Output{
			LockingScript: test.ContractLockingScript,
			Value:         2200,
		})
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&transferTxID, 0), make([]byte,
			txbuilder.MaximumP2PKHSigScriptSize)))

		// test.admin output
		tx.AddTxOut(wire.NewTxOut(50, test.AdminLockingScript))

		// recipient output
		tx.AddTxOut(wire.NewTxOut(50, lockingScript))

		settlement := &actions.Settlement{
			Instruments: []*actions.InstrumentSettlementField{
				{
					InstrumentType: currency.Code(),
					InstrumentCode: instrumentCode[:],
					Settlements: []*actions.QuantityIndexField{
						{
							Quantity: remainingQuantity,
							Index:    0,
						},
						{
							Quantity: quantity,
							Index:    1,
						},
					},
				},
			},
			Timestamp: uint64(time.Now().UnixNano()),
		}

		randCount := mathRand.Intn(3)
		for i := 0; i < randCount; i++ {
			key, err := bitcoin.GenerateKey(bitcoin.MainNet)
			if err != nil {
				t.Fatalf("Failed to generate key : %s", err)
			}

			lockingScript, err := key.LockingScript()
			if err != nil {
				t.Fatalf("Failed to create locking script : %s", err)
			}

			quantity := uint64(mathRand.Intn(100000) + 1)

			// recipient output
			index := len(tx.TxOut)
			tx.AddTxOut(wire.NewTxOut(50, lockingScript))

			settlement.Instruments[0].Settlements = append(settlement.Instruments[0].Settlements,
				&actions.QuantityIndexField{
					Quantity: quantity,
					Index:    uint32(index),
				})
		}

		settlementScript, err := protocol.Serialize(settlement, true)
		if err != nil {
			t.Fatalf("Failed to serialize settlement : %s", err)
		}

		settlementScriptOutputIndex := len(tx.TxOut)
		tx.AddTxOut(wire.NewTxOut(0, settlementScript))
		settlementTxID := *tx.TxHash()
		txids = append(txids, settlementTxID)

		settlementTx := &transactions.Transaction{
			Tx:           tx,
			State:        transactions.TxStateSafe,
			SpentOutputs: outputs,
		}

		settlementTx, err = test.Caches.Transactions.Add(ctx, settlementTx)
		if err != nil {
			t.Fatalf("Failed to add settlement tx : %s", err)
		}

		t.Logf("Sending transfer request : %s", settlementTxID)
		if err := agent.Process(ctx, settlementTx, []Action{{
			OutputIndex: settlementScriptOutputIndex,
			Action:      settlement,
			Agents: []ActionAgent{
				{
					LockingScript: test.ContractLockingScript,
					IsRequest:     false,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process settlement : %s", err)
		}
		t.Logf("Processed transfer request : %s", settlementTxID)
		test.Caches.Transactions.Release(ctx, settlementTxID)
	}

	// Check test.Caches.Balances
	t.Logf("Checking balances")
	for i := 0; i < recipientCount; i++ {
		if err := test.Caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		balance, err := test.Caches.Caches.Balances.Get(ctx, test.ContractLockingScript, instrumentCode,
			lockingScripts[i])
		if err != nil {
			t.Fatalf("Failed to get balance : %s", err)
		}

		if balance == nil {
			t.Fatalf("Missing balance")
		}

		t.Logf("Balance : %d", balance.Quantity)

		if balance.Quantity != quantities[i] {
			t.Errorf("Wrong balance quantity : got %d, want %d", balance.Quantity, quantities[i])
		}
		test.Caches.Caches.Balances.Release(ctx, test.ContractLockingScript, instrumentCode, balance)
	}

	// Check test.admin balance is correct.
	adminBalance, err = test.Caches.Caches.Balances.Get(ctx, test.ContractLockingScript, instrumentCode,
		test.AdminLockingScript)
	if err != nil {
		t.Fatalf("Failed to get test.admin balance : %s", err)
	}

	if adminBalance == nil {
		t.Fatalf("Missing test.admin balance")
	}

	t.Logf("Admin balance : %d", adminBalance.Quantity)

	if adminBalance.Quantity != remainingQuantity {
		t.Errorf("Wrong test.admin balance quantity : got %d, want %d", adminBalance.Quantity,
			remainingQuantity)
	}
	test.Caches.Caches.Balances.Release(ctx, test.ContractLockingScript, instrumentCode, adminBalance)

	// Transfer test.Caches.Balances from scripts
	var lockingScripts2 []bitcoin.Script
	var quantities2 []uint64
	var txids2 []bitcoin.Hash32
	for i := 0; i < recipientCount; i++ {
		if err := test.Caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to generate key : %s", err)
		}

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		quantity := uint64(mathRand.Intn(int(quantities[i])-1) + 1)

		lockingScripts2 = append(lockingScripts2, lockingScript)
		quantities2 = append(quantities2, quantity)

		outputs = nil
		tx = wire.NewMsgTx(1)

		// Contract input
		transferTx := wire.NewMsgTx(1)
		transfer := &actions.Transfer{}
		transferScript, _ := protocol.Serialize(transfer, true)
		transferTx.AddTxOut(wire.NewTxOut(0, transferScript))
		transferTx.AddTxOut(wire.NewTxOut(2200, test.ContractLockingScript))
		transferTxID := *transferTx.TxHash()

		transferTransaction := &transactions.Transaction{
			Tx:           transferTx,
			State:        transactions.TxStateSafe,
			SpentOutputs: outputs,
		}

		transferTransaction, err = test.Caches.Transactions.Add(ctx,
			transferTransaction)
		if err != nil {
			t.Fatalf("Failed to add transfer tx : %s", err)
		}
		test.Caches.Transactions.Release(ctx, transferTxID)

		outputs = append(outputs, &expanded_tx.Output{
			LockingScript: test.ContractLockingScript,
			Value:         2200,
		})
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&transferTxID, 0), make([]byte,
			txbuilder.MaximumP2PKHSigScriptSize)))

		// test.admin output
		tx.AddTxOut(wire.NewTxOut(50, lockingScripts[i]))

		// recipient output
		tx.AddTxOut(wire.NewTxOut(50, lockingScript))

		settlement := &actions.Settlement{
			Instruments: []*actions.InstrumentSettlementField{
				{
					InstrumentType: currency.Code(),
					InstrumentCode: instrumentCode[:],
					Settlements: []*actions.QuantityIndexField{
						{
							Quantity: quantities[i] - quantity,
							Index:    0,
						},
						{
							Quantity: quantity,
							Index:    1,
						},
					},
				},
			},
			Timestamp: uint64(time.Now().UnixNano()),
		}

		randCount := mathRand.Intn(3)
		for i := 0; i < randCount; i++ {
			key, err := bitcoin.GenerateKey(bitcoin.MainNet)
			if err != nil {
				t.Fatalf("Failed to generate key : %s", err)
			}

			lockingScript, err := key.LockingScript()
			if err != nil {
				t.Fatalf("Failed to create locking script : %s", err)
			}

			quantity := uint64(mathRand.Intn(100000) + 1)

			// recipient output
			index := len(tx.TxOut)
			tx.AddTxOut(wire.NewTxOut(50, lockingScript))

			settlement.Instruments[0].Settlements = append(settlement.Instruments[0].Settlements,
				&actions.QuantityIndexField{
					Quantity: quantity,
					Index:    uint32(index),
				})
		}

		settlementScript, err := protocol.Serialize(settlement, true)
		if err != nil {
			t.Fatalf("Failed to serialize settlement : %s", err)
		}

		settlementScriptOutputIndex := len(tx.TxOut)
		tx.AddTxOut(wire.NewTxOut(0, settlementScript))
		settlementTxID := *tx.TxHash()
		txids2 = append(txids2, settlementTxID)

		settlementTx := &transactions.Transaction{
			Tx:           tx,
			State:        transactions.TxStateSafe,
			SpentOutputs: outputs,
		}

		settlementTx, err = test.Caches.Transactions.Add(ctx, settlementTx)
		if err != nil {
			t.Fatalf("Failed to add settlement tx : %s", err)
		}

		if err := agent.Process(ctx, settlementTx, []Action{{
			OutputIndex: settlementScriptOutputIndex,
			Action:      settlement,
			Agents: []ActionAgent{
				{
					LockingScript: test.ContractLockingScript,
					IsRequest:     false,
				},
			},
		}}); err != nil {
			t.Fatalf("Failed to process settlement : %s", err)
		}
		test.Caches.Transactions.Release(ctx, settlementTxID)
	}

	for i := 0; i < recipientCount; i++ {
		if err := test.Caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		bothBalances, err := test.Caches.Caches.Balances.GetMulti(ctx, test.ContractLockingScript, instrumentCode,
			[]bitcoin.Script{lockingScripts[i], lockingScripts2[i]})
		if err != nil {
			t.Fatalf("Failed to get test.Caches.Balances : %s", err)
		}

		if len(bothBalances) != 2 {
			t.Fatalf("Missing test.Caches.Balances : %d", len(bothBalances))
		}

		t.Logf("Balances : %d, %d", bothBalances[0].Quantity, bothBalances[1].Quantity)

		if bothBalances[0].Quantity != quantities[i]-quantities2[i] {
			t.Errorf("Wrong balance quantity : got %d, want %d", bothBalances[0].Quantity,
				quantities[i]-quantities2[i])
		}

		if bothBalances[1].Quantity != quantities2[i] {
			t.Errorf("Wrong balance quantity : got %d, want %d", bothBalances[1].Quantity,
				quantities2[i])
		}

		test.Caches.Caches.Balances.ReleaseMulti(ctx, test.ContractLockingScript, instrumentCode, bothBalances)
	}

	StopTestAgent(ctx, t, test)
}
