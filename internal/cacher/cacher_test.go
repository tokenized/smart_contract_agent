package cacher

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

type TestItem struct {
	Value string

	isModified bool
	sync.Mutex
}

func Test_NotFound(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMockStorage()

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), 2, time.Second, 10, time.Second)
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
	notFound, err := cache.Get(ctx, GetTestItemPath(hash))
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

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), 2, time.Second, 10, time.Second)
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
	path := item.Path()

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

	cache.Release(ctx, path)

	gotCacheItem, err := cache.Get(ctx, path)
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

	cache.Release(ctx, path)

	duplicateItem := &TestItem{
		Value: "test value",
	}
	path = duplicateItem.Path()

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

	cache.Release(ctx, path)

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

	cache, err := NewCache(store, reflect.TypeOf(&TestItem{}), 2, time.Second, 10,
		100*time.Millisecond)
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
	path := item.Path()

	if _, err := cache.Add(ctx, item); err != nil {
		t.Fatalf("Failed to add item : %s", err)
	}

	cache.Release(ctx, path)

	gotCacheItem, err := cache.Get(ctx, path)
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
		// item was not expired and should be returned from memory
		t.Errorf("Got item should match")
	}

	if gotItem.Value != item.Value {
		t.Errorf("Wrong item value : got %s, want %s", gotItem.Value, item.Value)
	}

	cache.Release(ctx, path)

	time.Sleep(110 * time.Millisecond)

	gotCacheItem, err = cache.Get(ctx, path)
	if err != nil {
		t.Fatalf("Failed to get item : %s", err)
	}

	if gotCacheItem == nil {
		t.Fatalf("Item not found")
	}

	gotItem, ok = gotCacheItem.(*TestItem)
	if !ok {
		t.Fatalf("Got item not a TestItem")
	}

	if gotItem == item {
		// item was expired and should have been rebuilt from storage
		t.Errorf("Got item should not match")
	}

	if gotItem.Value != item.Value {
		t.Errorf("Wrong item value : got %s, want %s", gotItem.Value, item.Value)
	}

	cache.Release(ctx, path)

	close(interrupt)
	select {
	case <-time.After(time.Second):
		t.Errorf("Cache shutdown timed out")
	case <-cacheComplete:
	}
}

func GetTestItemPath(id bitcoin.Hash32) string {
	return fmt.Sprintf("items/%s", id)
}

func (i *TestItem) Path() string {
	return fmt.Sprintf("items/%s", bitcoin.Hash32(sha256.Sum256([]byte(i.Value))))
}

func (i *TestItem) IsModified() bool {
	return i.isModified
}

func (i *TestItem) ClearModified() {
	i.isModified = false
}

func (i *TestItem) Serialize(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(i.Value))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write([]byte(i.Value)); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func (i *TestItem) Deserialize(r io.Reader) error {
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
