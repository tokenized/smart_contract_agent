package state

import (
	"context"
	"math/rand"
	"testing"

	"github.com/tokenized/pkg/bitcoin"
	ci "github.com/tokenized/pkg/cacher"
	"github.com/tokenized/pkg/storage"
)

func Test_RecoveryTransactions(t *testing.T) {
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

	cacher := ci.NewSimpleCache(store)

	cache, err := NewRecoveryTransactionsCache(cacher)
	if err != nil {
		t.Fatalf("Failed to create recovery transactions cache : %s", err)
	}

	var txids []bitcoin.Hash32
	var outputIndexes [][]int

	for i := 0; i < 100; i++ {
		var txid bitcoin.Hash32
		rand.Read(txid[:])
		txids = append(txids, txid)

		count := rand.Intn(2) + 1
		outputs := make([]int, count)
		for j := range outputs {
			outputs[j] = rand.Intn(5)
		}
		outputIndexes = append(outputIndexes, outputs)

		newRecoveryTransaction := &RecoveryTransaction{
			TxID:          txid,
			OutputIndexes: outputs,
		}

		newRecoveryTransactions := &RecoveryTransactions{
			Transactions: []*RecoveryTransaction{newRecoveryTransaction},
		}

		added, err := cache.Add(ctx, contractLockingScript, newRecoveryTransactions)
		if err != nil {
			t.Fatalf("Failed to add recovery transactions : %s", err)
		}

		if i == 0 {
			if added != newRecoveryTransactions {
				t.Fatalf("Added recovery transactions not new")
			}
		} else {
			if added == newRecoveryTransactions {
				t.Fatalf("Added recovery transactions is new")
			}

			added.Append(newRecoveryTransaction)
		}

		cache.Release(ctx, contractLockingScript)
	}

	got, err := cache.Get(ctx, contractLockingScript)
	if err != nil {
		t.Fatalf("Failed to get recovery transactions : %s", err)
	}

	if got == nil {
		t.Fatalf("Recovery transactions not found")
	}

	got.Lock()
	if len(got.Transactions) != len(txids) {
		t.Fatalf("Wrong transaction count : got %d, want %d", len(got.Transactions), len(txids))
	}

	// Check values
	for i, txid := range txids {
		recoveryTx := got.Transactions[i]
		if !recoveryTx.TxID.Equal(&txid) {
			t.Errorf("Wrong txid %d : got %s, want %s", i, recoveryTx.TxID, txid)
		}

		if len(recoveryTx.OutputIndexes) != len(outputIndexes[i]) {
			t.Errorf("Wrong output index count %d : got %d, want %d", i,
				len(recoveryTx.OutputIndexes), len(outputIndexes[i]))
			continue
		}

		for j, outputIndex := range outputIndexes[i] {
			if recoveryTx.OutputIndexes[j] != outputIndex {
				t.Errorf("Wrong output index %d[%d] : got %d, want %d", i, j,
					recoveryTx.OutputIndexes[j], outputIndex)
			}
		}
	}

	// Remove one
	removeIndex := rand.Intn(len(txids))
	removeTxID := txids[removeIndex]
	for _, outputIndex := range outputIndexes[removeIndex] {
		if !got.RemoveOutput(removeTxID, outputIndex) {
			t.Errorf("Remove output returned false")
		}
	}
	txids = append(txids[:removeIndex], txids[removeIndex+1:]...)
	outputIndexes = append(outputIndexes[:removeIndex], outputIndexes[removeIndex+1:]...)

	// Re-check values
	for i, txid := range txids {
		recoveryTx := got.Transactions[i]
		if !recoveryTx.TxID.Equal(&txid) {
			t.Errorf("Wrong txid %d : got %s, want %s", i, recoveryTx.TxID, txid)
		}

		if len(recoveryTx.OutputIndexes) != len(outputIndexes[i]) {
			t.Errorf("Wrong output index count %d : got %d, want %d", i,
				len(recoveryTx.OutputIndexes), len(outputIndexes[i]))
			continue
		}

		for j, outputIndex := range outputIndexes[i] {
			if recoveryTx.OutputIndexes[j] != outputIndex {
				t.Errorf("Wrong output index %d[%d] : got %d, want %d", i, j,
					recoveryTx.OutputIndexes[j], outputIndex)
			}
		}
	}

	got.Unlock()
	cache.Release(ctx, contractLockingScript)

	if !cacher.IsEmpty() {
		t.Fatalf("Cacher is not empty")
	}
}
