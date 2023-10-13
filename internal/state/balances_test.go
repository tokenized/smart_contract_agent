package state

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/cacher"
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

	cacher := cacher.NewSimpleCache(store)

	cache, err := NewBalanceCache(cacher)
	if err != nil {
		t.Fatalf("Failed to create balance cache : %s", err)
	}

	for j := 0; j < 100; j++ {
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

		// js, _ := json.MarshalIndent(balances, "", "  ")
		// t.Logf("Balances : %s", js)

		addedBalances, err := cache.AddMulti(ctx, contractLockingScript, instrumentCode, balances)
		if err != nil {
			t.Fatalf("%d Failed to add balances : %s", j, err)
		}

		// js, _ = json.MarshalIndent(addedBalances, "", "  ")
		// t.Logf("Added balances : %s", js)
		cache.ReleaseMulti(ctx, contractLockingScript, instrumentCode, addedBalances)

		for i := 0; i < 5; i++ {
			gotBalance, err := cache.Get(ctx, contractLockingScript, instrumentCode,
				balances[i].LockingScript)
			if err != nil {
				t.Fatalf("%d Failed to get balance : %s", j, err)
			}

			// js, _ = json.MarshalIndent(gotBalance, "", "  ")
			// t.Logf("Got balance : %s", js)

			if gotBalance == nil {
				t.Fatalf("%d Got balance should not be nil", j)
			}

			if gotBalance.Quantity != balances[i].Quantity {
				t.Errorf("%d Wrong got balance quantity : got %d, want %d", j, gotBalance.Quantity,
					balances[i].Quantity)
			}
			cache.Release(ctx, contractLockingScript, instrumentCode, gotBalance)
		}

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
			t.Fatalf("%d Failed to add balances : %s", j, err)
		}

		// js, _ = json.MarshalIndent(mixedAddedBalances, "", "  ")
		// t.Logf("Mixed added balances : %s", js)

		if mixedAddedBalances[2].Quantity != balances[1].Quantity {
			t.Errorf("%d Wrong got balance quantity : got %d, want %d", j,
				mixedAddedBalances[2].Quantity, balances[1].Quantity)
		}

		mixedAddedBalances[0].Quantity = uint64(rand.Intn(5000))
		mixedAddedBalances[0].MarkModified()
		savedLockingScript := mixedAddedBalances[0].LockingScript
		savedQuantity := mixedAddedBalances[0].Quantity

		cache.ReleaseMulti(ctx, contractLockingScript, instrumentCode, mixedAddedBalances)

		gotBalance2, err := cache.Get(ctx, contractLockingScript, instrumentCode,
			savedLockingScript)
		if err != nil {
			t.Fatalf("%d Failed to get balance 2  : %s", j, err)
		}

		// js, _ = json.MarshalIndent(gotBalance2, "", "  ")
		// t.Logf("Got balance 2 : %s", js)

		if gotBalance2 == nil {
			t.Fatalf("%d Got balance 2 should not be nil", j)
		}

		if gotBalance2.Quantity != savedQuantity {
			t.Errorf("%d Wrong got balance 2 quantity : got %d, want %d", j, gotBalance2.Quantity,
				savedQuantity)
		}
		cache.Release(ctx, contractLockingScript, instrumentCode, gotBalance2)
	}

	if !cacher.IsEmpty(ctx) {
		t.Fatalf("Cacher is not empty")
	}
}
