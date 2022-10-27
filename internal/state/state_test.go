package state

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"

	"github.com/google/uuid"
)

func Test_CopyRecursive(t *testing.T) {
	ctx := logger.ContextWithLogger(context.Background(), true, true, "")
	store := storage.NewMockStorage()

	fromPrefix := "from"
	toPrefix := "to"
	var items []string
	var values []*bitcoin.Hash32
	for i := 0; i < 100; i++ {
		item := uuid.New().String()
		items = append(items, item)

		value := &bitcoin.Hash32{}
		rand.Read(value[:])
		values = append(values, value)

		if err := store.Write(ctx, fromPrefix+"/"+item, value[:], nil); err != nil {
			t.Fatalf("Failed to write item : %s", err)
		}
	}

	if err := CopyRecursive(ctx, store, fromPrefix, toPrefix); err != nil {
		t.Fatalf("Failed to copy recursive : %s", err)
	}

	for i, item := range items {
		b, err := store.Read(ctx, toPrefix+"/"+item)
		if err != nil {
			t.Fatalf("Failed to read item %d : %s", i, err)
		}

		if !bytes.Equal(b, values[i][:]) {
			t.Fatalf("Value %d doesn't match : got %x, want %x", i, b, values[i][:])
		}
	}
}
