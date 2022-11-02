package agents

import (
	"bytes"
	"context"
	"crypto/rand"
	mathRand "math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/channels/wallet"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/expanded_tx"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Process(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()
	broadcaster := state.NewMockTxBroadcaster()

	caches := state.StartTestCaches(ctx, t, store, cacher.DefaultConfig(), time.Second)

	contractKey, contractLockingScript, contractAddress := state.MockKey()
	_, feeLockingScript, _ := state.MockKey()
	_, _, entityAddress := state.MockKey()

	var keyHash bitcoin.Hash32
	rand.Read(keyHash[:])

	contract := &state.Contract{
		KeyHash:       keyHash,
		LockingScript: contractLockingScript,
	}

	var err error
	contract, err = caches.Caches.Contracts.Add(ctx, contract)
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}

	agent, err := NewAgent(contractKey, DefaultConfig(), contract, feeLockingScript, caches.Caches,
		store, broadcaster, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create agent : %s", err)
	}

	// Create a contract by processing contract formation.
	var outputs []*expanded_tx.Output
	tx := &wire.MsgTx{}

	// Contract input
	txid := &bitcoin.Hash32{}
	rand.Read(txid[:])
	outputs = append(outputs, &expanded_tx.Output{
		LockingScript: contractLockingScript,
		Value:         2200,
	})
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(txid, 0), make([]byte,
		txbuilder.MaximumP2PKHSigScriptSize)))

	// Contract output
	tx.AddTxOut(wire.NewTxOut(50, contractLockingScript))

	// Contract formation output
	adminKey, err := bitcoin.GenerateKey(bitcoin.MainNet)
	if err != nil {
		t.Fatalf("Failed to generate key : %s", err)
	}

	ra, err := adminKey.RawAddress()
	if err != nil {
		t.Fatalf("Failed to create raw address : %s", err)
	}

	adminLockingScript, err := adminKey.LockingScript()
	if err != nil {
		t.Fatalf("Failed to create locking script : %s", err)
	}

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

	tx.AddTxOut(wire.NewTxOut(0, contractFormationScript))
	contractFormationTxID := *tx.TxHash()

	contractFormationTx := &state.Transaction{
		Tx:           tx,
		State:        wallet.TxStateSafe,
		SpentOutputs: outputs,
		IsProcessed:  false,
	}

	contractFormationTx, err = caches.Caches.Transactions.Add(ctx, contractFormationTx)
	if err != nil {
		t.Fatalf("Failed to add contract formation tx : %s", err)
	}

	now := uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, contractFormationTx, []actions.Action{contractFormation},
		now); err != nil {
		t.Fatalf("Failed to process contract formation : %s", err)
	}
	caches.Caches.Transactions.Release(ctx, contractFormationTxID)

	// Check contract is correct.
	currentContract, err := caches.Caches.Contracts.Get(ctx, contractLockingScript)
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
	caches.Caches.Contracts.Release(ctx, contractLockingScript)

	// Create instrument by processing instrument creation.
	outputs = nil
	tx = &wire.MsgTx{}

	// Contract input
	txid = &bitcoin.Hash32{}
	rand.Read(txid[:])
	outputs = append(outputs, &expanded_tx.Output{
		LockingScript: contractLockingScript,
		Value:         2200,
	})
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(txid, 0), make([]byte,
		txbuilder.MaximumP2PKHSigScriptSize)))

	// Contract output
	tx.AddTxOut(wire.NewTxOut(50, contractLockingScript))

	// Instrument creation output
	currency := instruments.Currency{
		CurrencyCode: "USD",
		Precision:    2,
	}

	currencyBuf := &bytes.Buffer{}
	if err := currency.Serialize(currencyBuf); err != nil {
		t.Fatalf("Failed to serialize currency payload : %s", err)
	}

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

	tx.AddTxOut(wire.NewTxOut(0, instrumentCreationScript))
	instrumentCreationTxID := *tx.TxHash()

	instrumentCreationTx := &state.Transaction{
		Tx:           tx,
		State:        wallet.TxStateSafe,
		SpentOutputs: outputs,
		IsProcessed:  false,
	}

	instrumentCreationTx, err = caches.Caches.Transactions.Add(ctx, instrumentCreationTx)
	if err != nil {
		t.Fatalf("Failed to add instrument creation tx : %s", err)
	}

	now = uint64(time.Now().UnixNano())
	if err := agent.Process(ctx, instrumentCreationTx, []actions.Action{instrumentCreation},
		now); err != nil {
		t.Fatalf("Failed to process instrument creation : %s", err)
	}
	caches.Caches.Transactions.Release(ctx, instrumentCreationTxID)

	// Check instrument is correct.
	currentContract, err = caches.Caches.Contracts.Get(ctx, contractLockingScript)
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

		gotInstrument, err := caches.Caches.Instruments.Get(ctx, contractLockingScript,
			instrumentCode)
		if err != nil {
			t.Fatalf("Failed to get instrument : %s", err)
		}

		if gotInstrument == nil {
			t.Fatalf("Instrument missing")
		}
		caches.Caches.Instruments.Release(ctx, contractLockingScript, instrumentCode)

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
	caches.Caches.Contracts.Release(ctx, contractLockingScript)

	if err := caches.IsFailed(); err != nil {
		t.Fatalf("Cache failed : %s", err)
	}

	// Check admin balance is correct.
	adminBalance, err := caches.Caches.Balances.Get(ctx, contractLockingScript, instrumentCode,
		adminLockingScript)
	if err != nil {
		t.Fatalf("Failed to get admin balance : %s", err)
	}

	if adminBalance == nil {
		t.Fatalf("Missing admin balance")
	}

	t.Logf("Admin balance : %d", adminBalance.Quantity)

	if adminBalance.Quantity != authorizedQuantity {
		t.Errorf("Wrong admin balance quantity : got %d, want %d", adminBalance.Quantity,
			authorizedQuantity)
	}
	caches.Caches.Balances.Release(ctx, contractLockingScript, instrumentCode, adminBalance)

	if err := caches.IsFailed(); err != nil {
		t.Fatalf("Cache failed : %s", err)
	}

	// Transfer to a lot of locking scripts.
	var lockingScripts []bitcoin.Script
	var quantities []uint64
	var txids []bitcoin.Hash32
	remainingQuantity := authorizedQuantity
	recipientCount := 10
	for i := 0; i < recipientCount; i++ {
		if err := caches.IsFailed(); err != nil {
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
		tx = &wire.MsgTx{}

		// Contract input
		txid = &bitcoin.Hash32{}
		rand.Read(txid[:])
		outputs = append(outputs, &expanded_tx.Output{
			LockingScript: contractLockingScript,
			Value:         2200,
		})
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(txid, 0), make([]byte,
			txbuilder.MaximumP2PKHSigScriptSize)))

		// admin output
		tx.AddTxOut(wire.NewTxOut(50, adminLockingScript))

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

		tx.AddTxOut(wire.NewTxOut(0, settlementScript))
		settlementTxID := *tx.TxHash()
		txids = append(txids, settlementTxID)

		settlementTx := &state.Transaction{
			Tx:           tx,
			State:        wallet.TxStateSafe,
			SpentOutputs: outputs,
			IsProcessed:  false,
		}

		settlementTx, err = caches.Caches.Transactions.Add(ctx, settlementTx)
		if err != nil {
			t.Fatalf("Failed to add settlement tx : %s", err)
		}

		now := uint64(time.Now().UnixNano())
		if err := agent.Process(ctx, settlementTx, []actions.Action{settlement}, now); err != nil {
			t.Fatalf("Failed to process settlement : %s", err)
		}
		caches.Caches.Transactions.Release(ctx, settlementTxID)
	}

	// Check caches.Balances
	for i := 0; i < recipientCount; i++ {
		if err := caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		balance, err := caches.Caches.Balances.Get(ctx, contractLockingScript, instrumentCode,
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
		caches.Caches.Balances.Release(ctx, contractLockingScript, instrumentCode, balance)
	}

	// Check admin balance is correct.
	adminBalance, err = caches.Caches.Balances.Get(ctx, contractLockingScript, instrumentCode,
		adminLockingScript)
	if err != nil {
		t.Fatalf("Failed to get admin balance : %s", err)
	}

	if adminBalance == nil {
		t.Fatalf("Missing admin balance")
	}

	t.Logf("Admin balance : %d", adminBalance.Quantity)

	if adminBalance.Quantity != remainingQuantity {
		t.Errorf("Wrong admin balance quantity : got %d, want %d", adminBalance.Quantity,
			remainingQuantity)
	}
	caches.Caches.Balances.Release(ctx, contractLockingScript, instrumentCode, adminBalance)

	// Transfer caches.Balances from scripts
	var lockingScripts2 []bitcoin.Script
	var quantities2 []uint64
	var txids2 []bitcoin.Hash32
	for i := 0; i < recipientCount; i++ {
		if err := caches.IsFailed(); err != nil {
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
		tx = &wire.MsgTx{}

		// Contract input
		txid = &bitcoin.Hash32{}
		rand.Read(txid[:])
		outputs = append(outputs, &expanded_tx.Output{
			LockingScript: contractLockingScript,
			Value:         2200,
		})
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(txid, 0), make([]byte,
			txbuilder.MaximumP2PKHSigScriptSize)))

		// admin output
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

		tx.AddTxOut(wire.NewTxOut(0, settlementScript))
		settlementTxID := *tx.TxHash()
		txids2 = append(txids2, settlementTxID)

		settlementTx := &state.Transaction{
			Tx:           tx,
			State:        wallet.TxStateSafe,
			SpentOutputs: outputs,
			IsProcessed:  false,
		}

		settlementTx, err = caches.Caches.Transactions.Add(ctx, settlementTx)
		if err != nil {
			t.Fatalf("Failed to add settlement tx : %s", err)
		}

		now := uint64(time.Now().UnixNano())
		if err := agent.Process(ctx, settlementTx, []actions.Action{settlement}, now); err != nil {
			t.Fatalf("Failed to process settlement : %s", err)
		}
		caches.Caches.Transactions.Release(ctx, settlementTxID)
	}

	for i := 0; i < recipientCount; i++ {
		if err := caches.IsFailed(); err != nil {
			t.Fatalf("Cache failed : %s", err)
		}

		bothBalances, err := caches.Caches.Balances.GetMulti(ctx, contractLockingScript, instrumentCode,
			[]bitcoin.Script{lockingScripts[i], lockingScripts2[i]})
		if err != nil {
			t.Fatalf("Failed to get caches.Balances : %s", err)
		}

		if len(bothBalances) != 2 {
			t.Fatalf("Missing caches.Balances : %d", len(bothBalances))
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

		caches.Caches.Balances.ReleaseMulti(ctx, contractLockingScript, instrumentCode, bothBalances)
	}

	caches.Caches.Contracts.Release(ctx, contractLockingScript)

	caches.StopTestCaches()
}
