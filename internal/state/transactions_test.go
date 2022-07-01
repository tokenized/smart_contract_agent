package state

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/txbuilder"
	"github.com/tokenized/pkg/wire"
)

func Test_FetchTxs(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	cache, err := NewTransactionCache(store, 4, 2*time.Second, 10000, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create tx cache : %s", err)
	}

	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cache.Run(ctx, interrupt)
		close(cacheComplete)
	}()

	txCount := 1000
	t.Logf("Creating %d transactions", txCount)
	var txids []bitcoin.Hash32
	for i := 0; i < txCount; i++ {
		tx := wire.NewMsgTx(1)

		key, err := bitcoin.GenerateKey(bitcoin.MainNet)
		if err != nil {
			t.Fatalf("Failed to generate key : %s", err)
		}

		var hash bitcoin.Hash32
		rand.Read(hash[:])
		unlockingScript := make(bitcoin.Script, txbuilder.MaximumP2PKHSigScriptSize)
		rand.Read(unlockingScript)
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&hash, uint32(rand.Intn(5))), unlockingScript))

		lockingScript, err := key.LockingScript()
		if err != nil {
			t.Fatalf("Failed to create locking script : %s", err)
		}

		tx.AddTxOut(wire.NewTxOut(uint64(rand.Intn(1000000)), lockingScript))

		txids = append(txids, *tx.TxHash())

		if _, err := cache.AddRaw(ctx, tx, nil); err != nil {
			t.Fatalf("Failed to add tx : %s", err)
		}
		cache.Release(ctx, *tx.TxHash())
	}

	retrieveCount := 10000
	t.Logf("Retrieving %d transactions", retrieveCount)
	for i := 0; i < retrieveCount; i++ {
		index := rand.Intn(txCount)
		txid := txids[index]

		tx, err := cache.Get(ctx, txid)
		if err != nil {
			t.Fatalf("Failed to get tx : %s", err)
		}

		if tx == nil {
			t.Errorf("TxID not found %d : %s", index, txid)
			continue
		}

		gotTxID := *tx.Tx.TxHash()
		if !gotTxID.Equal(&txid) {
			t.Errorf("Wrong got txid %d : got %s, want %s", index, gotTxID, txid)
		}

		cache.Release(ctx, txid)
	}

	t.Logf("Finished retrieving")

	close(interrupt)
	select {
	case <-time.After(3 * time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}
