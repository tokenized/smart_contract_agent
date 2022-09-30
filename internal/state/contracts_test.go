package state

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/cacher"
	"github.com/tokenized/config"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/instruments"
)

func Test_Contracts(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	contractKey, err := bitcoin.GenerateKey(bitcoin.MainNet)
	if err != nil {
		t.Fatalf("Failed to create key : %s", err)
	}

	contractLockingScript, err := contractKey.LockingScript()
	if err != nil {
		t.Fatalf("Failed to create locking script : %s", err)
	}

	contractHash := CalculateContractHash(contractLockingScript)

	cacherConfig := cacher.DefaultConfig()
	cacherConfig.Expiration = config.Duration{time.Millisecond * 50}
	cacher := cacher.NewCache(store, cacherConfig)

	cache, err := NewContractCache(cacher)
	if err != nil {
		t.Fatalf("Failed to create contract cache : %s", err)
	}

	shutdown := make(chan error, 1)
	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cacher.Run(ctx, interrupt, shutdown)
		close(cacheComplete)
	}()

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
	rand.Read(contract.KeyHash[:])
	rand.Read(contract.FormationTxID[:])
	rand.Read(contract.BodyOfAgreementFormationTxID[:])

	// Add some instruments
	for i := 0; i < 3; i++ {
		var code InstrumentCode
		rand.Read(code[:])

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
			ContractHash:   contractHash,
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

		contract.Instruments = append(contract.Instruments, instrument)
	}

	addedContract, err := cache.Add(ctx, contract)
	if err != nil {
		t.Fatalf("Failed to add contract : %s", err)
	}

	if addedContract != contract {
		t.Errorf("Added contract should match contract")
	}

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

	if len(gotContract.Instruments) != 3 {
		t.Errorf("Wrong number of contracts : got %d, want %d", len(gotContract.Instruments), 3)
	}

	for i := 0; i < 3; i++ {
		instrument := gotContract.Instruments[i]

		if instrument.Creation == nil {
			t.Fatalf("Missing instrument creation")
		}

		if instrument.Creation.InstrumentIndex != uint64(i) {
			t.Errorf("Wrong instrument index : got %d, want %d",
				instrument.Creation.InstrumentIndex, uint64(i))
		}

		payload, err := instrument.GetInstrument()
		if err != nil {
			t.Fatalf("Failed to get instrument payload : %s", err)
		}

		cur, ok := payload.(*instruments.Currency)
		if !ok {
			t.Fatalf("Instrument payload is not currency")
		}

		if cur.CurrencyCode != "USD" {
			t.Errorf("Wrong currency code : got %s, want %s", cur.CurrencyCode, "USD")
		}

		if cur.Precision != 2 {
			t.Errorf("Wrong precision : got %d, want %d", cur.Precision, 2)
		}
	}

	cache.Release(ctx, contractLockingScript)

	close(interrupt)
	select {
	case <-time.After(time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}
