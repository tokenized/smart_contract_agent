package cacher

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/threads"

	"github.com/pkg/errors"
)

var (
	ErrTimedOut     = errors.New("Timed Out")
	ErrShuttingDown = errors.New("Shutting Down")
)

// CacheValue represents the item being cached. It must be a pointer type that implements these
// functions.
type CacheValue interface {
	// Object has been modified since last write to storage.
	IsModified() bool
	ClearModified()

	// Storage path and serialization.
	Path() string
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error

	// All functions will be called by cache while the object is locked.
	Lock()
	Unlock()
}

// Cache is a locked caching system that retrieves values from storage and keeps them in memory for
// a certain amount of time after they are no longer used in case they are used again.
// The users of cache must be stopped before stopping the cache so that the cache can complete any
// active requests. This is needed to prevent partial value updates.
type Cache struct {
	typ reflect.Type

	store storage.StreamStorage

	requestThreadCount int
	requestTimeout     time.Duration
	requestTimeoutLock sync.Mutex
	requests           chan *Request

	saving chan *CacheItem

	expireThreadCount int
	expiration        time.Duration
	expirers          chan *ExpireItem

	items        map[string]*CacheItem
	itemsLock    sync.Mutex
	itemsAddLock sync.Mutex

	itemsUseCount     int
	itemsUseCountLock sync.Mutex
}

type CacheItem struct {
	value CacheValue

	isNew    bool
	users    uint
	lastUsed time.Time
	sync.Mutex
}

func NewCache(store storage.StreamStorage, typ reflect.Type, requestThreadCount int,
	requestTimeout time.Duration, expireCount int, expiration time.Duration) (*Cache, error) {

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must be support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(CacheValue); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &Cache{
		typ:                typ,
		store:              store,
		requestThreadCount: requestThreadCount,
		requestTimeout:     requestTimeout,
		requests:           make(chan *Request, 100),
		saving:             make(chan *CacheItem, 100),
		expireThreadCount:  4,
		expiration:         expiration,
		expirers:           make(chan *ExpireItem, expireCount),
		items:              make(map[string]*CacheItem),
	}, nil
}

func (c *Cache) RequestTimeout() time.Duration {
	c.requestTimeoutLock.Lock()
	defer c.requestTimeoutLock.Unlock()

	return c.requestTimeout
}

// Add adds an item if it isn't in the cache or storage yet. If it is already in the cache or
// storage then it returns the existing item and ignores the item parameter. This must be done this
// way so that there is a lock across checking if the item already exists and adding the item.
// Otherwise there can be conflicts in adding the item from multiple threads.
func (c *Cache) Add(ctx context.Context, value CacheValue) (CacheValue, error) {
	path := value.Path()

	// TODO This might be slow to add items since it requires a hit to storage to ensure it isn't
	// already there. But that might be unavoidable. --ce
	c.itemsAddLock.Lock()
	defer c.itemsAddLock.Unlock()

	gotItem, err := c.Get(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if gotItem != nil {
		return gotItem, nil // already had item
	}

	newItem := &CacheItem{
		isNew: true,
		value: value,
	}
	c.addUser(newItem)

	c.itemsLock.Lock()
	c.items[path] = newItem
	c.itemsLock.Unlock()

	return value, nil
}

func (c *Cache) AddMulti(ctx context.Context, values []CacheValue) ([]CacheValue, error) {
	paths := make([]string, len(values))
	for i, value := range values {
		paths[i] = value.Path()
	}

	// TODO This might be slow to add items since it requires a hit to storage to ensure it isn't
	// already there. But that might be unavoidable. --ce
	c.itemsAddLock.Lock()
	defer c.itemsAddLock.Unlock()

	gotItems, err := c.GetMulti(ctx, paths)
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	result := make([]CacheValue, len(values))
	for i, gotItem := range gotItems {
		if gotItem != nil {
			result[i] = gotItem // already had item
			continue
		}

		newItem := &CacheItem{
			isNew: true,
			value: values[i],
		}
		c.addUser(newItem)
		result[i] = values[i]

		c.itemsLock.Lock()
		c.items[paths[i]] = newItem
		c.itemsLock.Unlock()
	}

	return result, nil
}

// Get gets an item from the cache and if it isn't in the cache it attempts to read it from storage.
// If it is found in storage it is added to the cache. If it isn't found in the cache or storage
// then (nil, nil) is returned. Release must be called after the item is no longer used to allow it
// to expire from the cache.
func (c *Cache) Get(ctx context.Context, path string) (CacheValue, error) {
	response, err := c.createRequest(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}

	// Wait for the item to be retrieved.
	timeout := c.RequestTimeout()
	select {
	case r := <-response:
		if r == nil {
			return nil, nil
		}

		if item, ok := r.(*CacheItem); ok {
			return item.value, nil
		}

		return nil, r.(error)

	case <-time.After(timeout):
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.String("path", path),
		}, "Get timed out")
		return nil, errors.Wrapf(ErrTimedOut, "%s", timeout)
	}
}

func (c *Cache) GetMulti(ctx context.Context, paths []string) ([]CacheValue, error) {
	// Create all requests
	responses := make([]<-chan interface{}, len(paths))
	for i, path := range paths {
		response, err := c.createRequest(ctx, path)
		if err != nil {
			return nil, errors.Wrap(err, "request")
		}
		responses[i] = response
	}

	// Wait for the items to be retrieved.
	result := make([]CacheValue, len(paths))
	timeout := c.RequestTimeout()
	for i, response := range responses {
		select {
		case r := <-response:
			if r == nil {
				result[i] = nil
				continue
			}

			if item, ok := r.(*CacheItem); ok {
				result[i] = item.value
				continue
			}

			return nil, r.(error)

		case <-time.After(timeout):
			logger.ErrorWithFields(ctx, []logger.Field{
				logger.String("path", paths[i]),
			}, "Get multi timed out")
			return nil, errors.Wrapf(ErrTimedOut, "%s", timeout)
		}
	}

	return result, nil
}

// Save adds an item to the saving thread to be saved to storage if it is modified or new. This
// automatically happens on release of the item, but this function can be called to ensure the
// object is written even if it is held onto for a long time.
func (c *Cache) Save(ctx context.Context, value CacheValue) {
	value.Lock()
	path := value.Path()
	isModified := value.IsModified()
	value.Unlock()

	c.itemsLock.Lock()
	item, exists := c.items[path]
	if !exists {
		c.itemsLock.Unlock()
		panic(fmt.Sprintf("Item saved when not in cache: %s\n", path))
	}
	item.Lock()
	isNew := item.isNew
	item.Unlock()
	c.itemsLock.Unlock()

	if isNew || isModified {
		c.addUserNoLock(item) // add user until save completes
		c.saving <- item
	}
}

// Release notifies the cache that the item is no longer being used and can be expired. When there
// are no users remaining then the item will be set to expire
func (c *Cache) Release(ctx context.Context, path string) {
	c.itemsLock.Lock()
	item, exists := c.items[path]
	if !exists {
		c.itemsLock.Unlock()
		panic(fmt.Sprintf("Item released when not in cache: %s\n", path))
	}
	item.Lock()
	c.itemsLock.Unlock()

	c.releaseItem(ctx, item, path)
	item.Unlock()
}

func (c *Cache) releaseItem(ctx context.Context, item *CacheItem, path string) {
	item.value.Lock()
	isModified := item.value.IsModified()
	item.value.Unlock()

	if item.isNew || isModified {
		c.addUserNoLock(item) // add user until save completes
		c.saving <- item
	}

	if item.users == 0 {
		panic(fmt.Sprintf("Item released with no users: %s\n", path))
	}

	item.users--
	if item.users > 0 {
		return
	}

	c.itemsUseCountLock.Lock()
	c.itemsUseCount--
	c.itemsUseCountLock.Unlock()

	// Add to expirers
	now := time.Now()
	item.lastUsed = now

	c.expirers <- &ExpireItem{
		item:     item,
		lastUsed: now,
	}
}

func (c *Cache) addUserNoLock(item *CacheItem) {
	if item.users == 0 {
		c.itemsUseCountLock.Lock()
		c.itemsUseCount++
		c.itemsUseCountLock.Unlock()
	}

	item.users++
}

func (c *Cache) addUser(item *CacheItem) {
	item.Lock()
	defer item.Unlock()

	if item.users == 0 {
		c.itemsUseCountLock.Lock()
		c.itemsUseCount++
		c.itemsUseCountLock.Unlock()
	}

	item.users++
}

// Run runs threads that fetch items from storage and expire items.
func (c *Cache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	errs := make([]error, c.requestThreadCount+c.expireThreadCount+1)
	selects := make([]reflect.SelectCase, c.requestThreadCount+c.expireThreadCount+2)
	selectIndex := 0
	var wait sync.WaitGroup

	requestsInterrupts := make([]chan interface{}, c.requestThreadCount)
	for i := 0; i < c.requestThreadCount; i++ {
		requestsComplete := make(chan interface{})
		requestsInterrupt := make(chan interface{})
		requestsInterrupts[i] = requestsInterrupt
		requestSelectIndex := selectIndex
		requestIndex := i
		wait.Add(1)
		go func() {
			errs[requestSelectIndex] = c.runHandleRequests(ctx, requestsInterrupt, requestIndex)
			close(requestsComplete)
			wait.Done()
		}()

		selects[selectIndex] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(requestsComplete),
		}
		selectIndex++
	}

	saveComplete := make(chan interface{})
	saveInterrupt := make(chan interface{})
	saveIndex := selectIndex
	wait.Add(1)
	go func() {
		errs[saveIndex] = c.runSaveItems(ctx, saveInterrupt)
		close(saveComplete)
		wait.Done()
	}()

	selects[selectIndex] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(saveComplete),
	}
	selectIndex++

	expireInterrupts := make([]chan interface{}, c.expireThreadCount)
	for i := 0; i < c.expireThreadCount; i++ {
		expireComplete := make(chan interface{})
		expireInterrupt := make(chan interface{})
		expireInterrupts[i] = expireInterrupt
		expireSelectIndex := selectIndex
		expireIndex := i
		wait.Add(1)
		go func() {
			errs[expireSelectIndex] = c.runExpireItems(ctx, expireInterrupt, expireIndex)
			close(expireComplete)
			wait.Done()
		}()

		selects[selectIndex] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(expireComplete),
		}
		selectIndex++
	}

	interruptIndex := selectIndex
	selects[selectIndex] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(interrupt),
	}

	index, _, _ := reflect.Select(selects)
	if index < saveIndex {
		logger.Error(ctx, "Cache Handle Requests %d Completed : %s", index, errs[index])
	} else if index == saveIndex {
		logger.Error(ctx, "Cache Save %d Completed : %s", index, errs[index])
	} else if index < interruptIndex {
		logger.Error(ctx, "Cache Expire %d Completed : %s", index, errs[index])
	}

	logger.Verbose(ctx, "Shutting down cache")

	// Stop runHandleRequests threads
	for _, requestsInterrupt := range requestsInterrupts {
		close(requestsInterrupt)
	}
	var finishHandlingRequestsErr error
	wait.Add(1)
	go func() {
		finishHandlingRequestsErr = c.finishHandlingRequests(ctx)
		close(c.requests)
		wait.Done()
	}()

	// Stop runSaveItems thread
	close(saveInterrupt)
	var finishSavingErr error
	wait.Add(1)
	go func() {
		finishSavingErr = c.finishSaving(ctx)
		close(c.saving)
		wait.Done()
	}()

	// Stop runExpireItems thread
	for _, expireInterrupt := range expireInterrupts {
		close(expireInterrupt)
	}
	wait.Add(1)
	var finishExpiringErr error
	go func() {
		finishExpiringErr = c.finishExpiring(ctx)
		close(c.expirers)
		wait.Done()
	}()

	wait.Wait()

	if finishHandlingRequestsErr != nil {
		errs = append(errs, finishHandlingRequestsErr)
	}
	if finishSavingErr != nil {
		errs = append(errs, finishSavingErr)
	}
	if finishExpiringErr != nil {
		errs = append(errs, finishExpiringErr)
	}

	return threads.CombineErrors(errs...)
}
