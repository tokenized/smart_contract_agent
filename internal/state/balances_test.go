package state

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
)

func Test_Balances(t *testing.T) {
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

	var instrumentCode InstrumentCode
	rand.Read(instrumentCode[:])

	cache, err := NewBalanceCache(store, 4, 10000, 2*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create balance cache : %s", err)
	}

	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cache.Run(ctx, interrupt)
		close(cacheComplete)
	}()

	var txid bitcoin.Hash32
	rand.Read(txid[:])
	var balances []*Balance
	for i := 0; i < 5; i++ {
		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to create key : %s", err)
		}

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		balances = append(balances, &Balance{
			LockingScript: lockingScript,
			Quantity:      uint64(rand.Intn(100)),
			Timestamp:     uint64(time.Now().UnixNano()),
			TxID:          &txid,
		})
	}

	js, _ := json.MarshalIndent(balances, "", "  ")
	t.Logf("Balances : %s", js)

	addedBalances, err := cache.AddMulti(ctx, contractLockingScript, instrumentCode, balances)
	if err != nil {
		t.Fatalf("Failed to add balances : %s", err)
	}

	js, _ = json.MarshalIndent(addedBalances, "", "  ")
	t.Logf("Added balances : %s", js)
	cache.ReleaseMulti(ctx, contractLockingScript, instrumentCode, addedBalances)

	gotBalance, err := cache.Get(ctx, contractLockingScript, instrumentCode,
		balances[0].LockingScript)
	if err != nil {
		t.Fatalf("Failed to get balance : %s", err)
	}

	js, _ = json.MarshalIndent(gotBalance, "", "  ")
	t.Logf("Got balance : %s", js)

	if gotBalance == nil {
		t.Fatalf("Got balance should not be nil")
	}

	if gotBalance.Quantity != balances[0].Quantity {
		t.Errorf("Wrong got balance quantity : got %d, want %d", gotBalance.Quantity,
			balances[0].Quantity)
	}
	cache.Release(ctx, contractLockingScript, instrumentCode, gotBalance.LockingScript)

	var mixedBalances []*Balance
	for i := 0; i < 2; i++ {
		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to create key : %s", err)
		}

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		mixedBalances = append(mixedBalances, &Balance{
			LockingScript: lockingScript,
			Quantity:      uint64(rand.Intn(100)),
			Timestamp:     uint64(time.Now().UnixNano()),
			TxID:          &txid,
		})
	}

	mixedBalances = append(mixedBalances, balances[1])

	for i := 0; i < 2; i++ {
		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to create key : %s", err)
		}

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		mixedBalances = append(mixedBalances, &Balance{
			LockingScript: lockingScript,
			Quantity:      uint64(rand.Intn(100)),
			Timestamp:     uint64(time.Now().UnixNano()),
			TxID:          &txid,
		})
	}

	mixedAddedBalances, err := cache.AddMulti(ctx, contractLockingScript, instrumentCode,
		mixedBalances)
	if err != nil {
		t.Fatalf("Failed to add balances : %s", err)
	}

	js, _ = json.MarshalIndent(mixedAddedBalances, "", "  ")
	t.Logf("Mixed added balances : %s", js)

	if mixedAddedBalances[2].Quantity != balances[1].Quantity {
		t.Errorf("Wrong got balance quantity : got %d, want %d", mixedAddedBalances[2].Quantity,
			balances[1].Quantity)
	}

	mixedAddedBalances[0].Quantity = uint64(rand.Intn(5000))
	savedLockingScript := mixedAddedBalances[0].LockingScript
	savedQuantity := mixedAddedBalances[0].Quantity
	if err := cache.Save(ctx, contractLockingScript, instrumentCode, mixedAddedBalances[0]); err != nil {
		t.Fatalf("Failed to save balance : %s", err)
	}

	cache.ReleaseMulti(ctx, contractLockingScript, instrumentCode, mixedAddedBalances)

	gotBalance2, err := cache.Get(ctx, contractLockingScript, instrumentCode, savedLockingScript)
	if err != nil {
		t.Fatalf("Failed to get balance 2 : %s", err)
	}

	js, _ = json.MarshalIndent(gotBalance2, "", "  ")
	t.Logf("Got balance 2 : %s", js)

	if gotBalance2 == nil {
		t.Fatalf("Got balance 2 should not be nil")
	}

	if gotBalance2.Quantity != savedQuantity {
		t.Errorf("Wrong got balance 2 quantity : got %d, want %d", gotBalance2.Quantity,
			savedQuantity)
	}
	cache.Release(ctx, contractLockingScript, instrumentCode, gotBalance2.LockingScript)

	close(interrupt)
	select {
	case <-time.After(3 * time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}