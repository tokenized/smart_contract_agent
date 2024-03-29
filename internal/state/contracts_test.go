package state

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
	"github.com/tokenized/specification/dist/golang/protocol"
)

func Test_Contract_Serialize(t *testing.T) {
	_, contractLockingScript, _ := MockKey()

	contract := &Contract{
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName: "Test Contract Name",
			Issuer: &actions.EntityField{
				Name: "John Bitcoin",
				Type: actions.EntitiesIndividual,
			},
			Timestamp: uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
		BodyOfAgreementFormation: &actions.BodyOfAgreementFormation{
			Chapters: []*actions.ChapterField{
				{
					Title:    "Chapter 1",
					Preamble: "This is the first chapter",
				},
			},
			Revision:  0,
			Timestamp: uint64(time.Now().UnixNano()),
		},
		BodyOfAgreementFormationTxID: &bitcoin.Hash32{},

		// Instruments []*Instrument `bsor:"7" json:"instruments"`
	}

	buf := &bytes.Buffer{}
	if err := contract.Serialize(buf); err != nil {
		t.Fatalf("Failed to serialize contract : %s", err)
	}

	newContract := &Contract{}
	if err := newContract.Deserialize(buf); err != nil {
		t.Fatalf("Failed to deserialize contract : %s", err)
	}

	js, _ := json.MarshalIndent(newContract, "", "  ")
	t.Logf("Contract : %s", js)
}

func Test_Contracts(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	_, contractLockingScript, contractAddress := MockKey()

	cacher := cacher.NewSimpleCache(store)

	cache, err := NewContractCache(cacher)
	if err != nil {
		t.Fatalf("Failed to create contract cache : %s", err)
	}

	instrumentCache, err := NewInstrumentCache(cacher)
	if err != nil {
		t.Fatalf("Failed to create instrument cache : %s", err)
	}

	// Create contract
	contract := &Contract{
		LockingScript: contractLockingScript,
		Formation: &actions.ContractFormation{
			ContractName: "Test Contract Name",
			Issuer: &actions.EntityField{
				Name: "John Bitcoin",
				Type: actions.EntitiesIndividual,
			},
			Timestamp: uint64(time.Now().UnixNano()),
		},
		FormationTxID: &bitcoin.Hash32{},
		BodyOfAgreementFormation: &actions.BodyOfAgreementFormation{
			Chapters: []*actions.ChapterField{
				{
					Title:    "Chapter 1",
					Preamble: "This is the first chapter",
				},
			},
			Revision:  0,
			Timestamp: uint64(time.Now().UnixNano()),
		},
		BodyOfAgreementFormationTxID: &bitcoin.Hash32{},

		// Instruments []*Instrument `bsor:"7" json:"instruments"`
	}
	rand.Read(contract.FormationTxID[:])
	rand.Read(contract.BodyOfAgreementFormationTxID[:])

	addedContract, err := cache.Add(ctx, contract)
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}

	if addedContract != contract {
		t.Errorf("Added contract should match contract")
	}

	contract.Lock()

	// Add some instruments
	for i := 0; i < 3; i++ {
		var code InstrumentCode

		nextInstrumentCode := protocol.InstrumentCodeFromContract(contractAddress,
			contract.InstrumentCount)
		contract.InstrumentCount++

		copy(code[:], nextInstrumentCode[:])

		currency := &instruments.Currency{
			CurrencyCode: "USD",
			Precision:    2,
		}

		payload, err := currency.Bytes()
		if err != nil {
			t.Fatalf("Failed to serialize instrument payload : %s", err)
		}

		instrument := &Instrument{
			InstrumentCode: code,
			Creation: &actions.InstrumentCreation{
				InstrumentCode:    []byte(code[:]),
				InstrumentIndex:   uint64(i),
				InstrumentType:    instruments.CodeCurrency,
				InstrumentPayload: payload,
				Timestamp:         uint64(time.Now().UnixNano()),
			},
			CreationTxID: &bitcoin.Hash32{},
		}

		copy(instrument.InstrumentType[:], []byte(instruments.CodeCurrency))
		rand.Read(instrument.CreationTxID[:])

		addedInstrument, err := instrumentCache.Add(ctx, contractLockingScript, instrument)
		if err != nil {
			t.Fatalf("Failed to add instrument : %s", err)
		}

		if addedInstrument != instrument {
			t.Errorf("Added instrument should match instrument")
		}

		instrumentCache.Release(ctx, contractLockingScript, code)
	}

	contract.Unlock()
	cache.Release(ctx, contractLockingScript)

	time.Sleep(time.Millisecond * 100) // expire contract so it is removed from cache

	gotContract, err := cache.Get(ctx, contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get contract : %s", err)
	}

	if gotContract == contract {
		// It shouldn't match because it should have been recreated from storage.
		t.Fatalf("Got contract should not match original contract")
	}

	js, _ := json.MarshalIndent(gotContract, "", "  ")
	t.Logf("Got Contract : %s", js)

	if gotContract.Formation == nil {
		t.Fatalf("Contract missing formation")
	}

	if gotContract.Formation.ContractName != contract.Formation.ContractName {
		t.Errorf("Wrong contract name : got \"%s\", want \"%s\"",
			gotContract.Formation.ContractName, contract.Formation.ContractName)
	}

	if gotContract.Formation.Timestamp != contract.Formation.Timestamp {
		t.Errorf("Wrong formation timestamp : got %d, want %d", gotContract.Formation.Timestamp,
			contract.Formation.Timestamp)
	}

	if gotContract.BodyOfAgreementFormation == nil {
		t.Fatalf("Contract missing body of agreement formation")
	}

	if gotContract.BodyOfAgreementFormation.Timestamp != contract.BodyOfAgreementFormation.Timestamp {
		t.Errorf("Wrong body of agreement timestamp : got %d, want %d",
			gotContract.BodyOfAgreementFormation.Timestamp,
			contract.BodyOfAgreementFormation.Timestamp)
	}

	if gotContract.InstrumentCount != 3 {
		t.Errorf("Wrong number of contracts : got %d, want %d", gotContract.InstrumentCount, 3)
	}

	cache.Release(ctx, contractLockingScript)

	if !cacher.IsEmpty(ctx) {
		t.Fatalf("Cacher is not empty")
	}
}
