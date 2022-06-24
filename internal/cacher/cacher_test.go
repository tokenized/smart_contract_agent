package cacher

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
)

type TestItem struct {
	Value string

	lock sync.Mutex
}

func Test_NotFound(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), "test_items", 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to create cache : %s", err)
	}

	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cache.Run(ctx, interrupt)
		close(cacheComplete)
	}()

	var hash bitcoin.Hash32
	rand.Read(hash[:])
	notFound, err := cache.Get(ctx, hash)
	if err != nil {
		t.Fatalf("Failed to get item : %s", err)
	}

	if notFound != nil {
		t.Fatalf("Item should be nil")
	}

	close(interrupt)
	select {
	case <-time.After(time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}

func Test_Add(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), "test_items", 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to create cache : %s", err)
	}

	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cache.Run(ctx, interrupt)
		close(cacheComplete)
	}()

	item := &TestItem{
		Value: "test value",
	}
	id := item.ID()

	addedCacheItem, err := cache.Add(ctx, item)
	if err != nil {
		t.Fatalf("Failed to add item : %s", err)
	}

	if addedCacheItem == nil {
		t.Fatalf("Added item should not be nil")
	}

	addedItem, ok := addedCacheItem.(*TestItem)
	if !ok {
		t.Fatalf("Added item not a TestItem")
	}

	if addedItem != item {
		t.Errorf("Wrong added item : got %s, want %s", addedItem.Value, item.Value)
	}

	gotCacheItem, err := cache.Get(ctx, id)
	if err != nil {
		t.Fatalf("Failed to get item : %s", err)
	}

	if gotCacheItem == nil {
		t.Fatalf("Item not found")
	}

	gotItem, ok := gotCacheItem.(*TestItem)
	if !ok {
		t.Fatalf("Got item not a TestItem")
	}

	if gotItem != item {
		t.Errorf("Wrong item found : got %s, want %s", gotItem.Value, item.Value)
	}

	cache.Release(ctx, id)

	duplicateItem := &TestItem{
		Value: "test value",
	}
	id = duplicateItem.ID()

	addedCacheItem, err = cache.Add(ctx, duplicateItem)
	if err != nil {
		t.Fatalf("Failed to add item : %s", err)
	}

	if addedCacheItem == nil {
		t.Fatalf("Added item should not be nil")
	}

	addedItem, ok = addedCacheItem.(*TestItem)
	if !ok {
		t.Fatalf("Added item not a TestItem")
	}

	if addedItem != item {
		t.Errorf("Wrong added item : got %s, want %s", addedItem.Value, item.Value)
	}

	close(interrupt)
	select {
	case <-time.After(time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}

func Test_Expire(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), "test_items", 10, time.Second)
	if err != nil {
		t.Fatalf("Failed to create cache : %s", err)
	}

	interrupt := make(chan interface{})
	cacheComplete := make(chan interface{})
	go func() {
		cache.Run(ctx, interrupt)
		close(cacheComplete)
	}()

	item := &TestItem{
		Value: "test value",
	}
	id := item.ID()

	if _, err := cache.Add(ctx, item); err != nil {
		t.Fatalf("Failed to add item : %s", err)
	}

	cache.Release(ctx, id)

	time.Sleep(1100 * time.Millisecond)

	gotCacheItem, err := cache.Get(ctx, id)
	if err != nil {
		t.Fatalf("Failed to get item : %s", err)
	}

	if gotCacheItem == nil {
		t.Fatalf("Item not found")
	}

	gotItem, ok := gotCacheItem.(*TestItem)
	if !ok {
		t.Fatalf("Got item not a TestItem")
	}

	if gotItem == item {
		t.Errorf("Got item should not match because it was expired and should have been rebuilt from storage")
	}

	if gotItem.Value != item.Value {
		t.Errorf("Wrong item value : got %s, want %s", gotItem.Value, item.Value)
	}

	close(interrupt)
	select {
	case <-time.After(time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}

func (i *TestItem) ID() bitcoin.Hash32 {
	i.lock.Lock()
	defer i.lock.Unlock()

	return bitcoin.Hash32(sha256.Sum256([]byte(i.Value)))
}

func (i *TestItem) Serialize(w io.Writer) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if err := binary.Write(w, binary.LittleEndian, uint32(len(i.Value))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write([]byte(i.Value)); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func (i *TestItem) Deserialize(r io.Reader) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "value")
	}
	i.Value = string(b)

	return nil
}
