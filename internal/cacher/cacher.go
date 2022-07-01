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
	Path() string
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error
}

type Cache struct {
	typ reflect.Type

	store storage.StreamStorage

	expiration     time.Duration
	expirationLock sync.Mutex

	requestThreadCount int
	requestTimeout     time.Duration
	requestTimeoutLock sync.Mutex

	requests     chan *Request
	requestsLock sync.Mutex

	items        map[string]*CacheItem
	itemsLock    sync.Mutex
	itemsAddLock sync.Mutex

	// TODO Add function to check if expires is full and being waited on, then don't wait for
	// expiration. This also limits the number of items in memory.
	expirers   chan *ExpireItem
	expireLock sync.Mutex
}

type CacheItem struct {
	value CacheValue

	users    uint
	lastUsed time.Time
	sync.Mutex
}

func NewCache(store storage.StreamStorage, typ reflect.Type, requestThreadCount, expireCount int,
	expiration, requestTimeout time.Duration) (*Cache, error) {

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
		expiration:         expiration,
		requestThreadCount: requestThreadCount,
		requestTimeout:     requestTimeout,
		requests:           make(chan *Request, 100),
		items:              make(map[string]*CacheItem),
		expirers:           make(chan *ExpireItem, expireCount),
	}, nil
}

func (c *Cache) RequestTimeout() time.Duration {
	c.requestTimeoutLock.Lock()
	defer c.requestTimeoutLock.Unlock()

	return c.requestTimeout
}

func (c *Cache) Save(ctx context.Context, value CacheValue) error {
	path := value.Path()
	if err := storage.StreamWrite(ctx, c.store, path, value); err != nil {
		return errors.Wrapf(err, "write %s", path)
	}

	return nil
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
		value:    value,
		users:    1,
		lastUsed: time.Now(),
	}

	c.itemsLock.Lock()
	c.items[path] = newItem
	c.itemsLock.Unlock()

	if err := c.Save(ctx, value); err != nil {
		return nil, errors.Wrap(err, "save")
	}

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
			value:    values[i],
			users:    1,
			lastUsed: time.Now(),
		}
		result[i] = values[i]

		c.itemsLock.Lock()
		c.items[paths[i]] = newItem
		c.itemsLock.Unlock()

		if err := c.Save(ctx, values[i]); err != nil {
			return nil, errors.Wrap(err, "save")
		}
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
			item.addUser()
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
				item.addUser()
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

	if item.users == 0 {
		item.Unlock()
		panic(fmt.Sprintf("Item released with no users: %s\n", path))
	}

	item.users--
	if item.users > 0 {
		item.Unlock()
		return
	}

	// Add to expirers
	now := time.Now()
	item.lastUsed = now
	item.Unlock()

	c.expireLock.Lock()
	if c.expirers != nil {
		c.expirers <- &ExpireItem{
			item:     item,
			lastUsed: now,
		}
	}
	c.expireLock.Unlock()
}

// Run runs threads that fetch items from storage and expire items.
func (c *Cache) Run(ctx context.Context, interrupt <-chan interface{}) error {
	errors := make([]error, c.requestThreadCount+1)
	selects := make([]reflect.SelectCase, c.requestThreadCount+2)
	selectIndex := 0
	var wait sync.WaitGroup

	for ; selectIndex < c.requestThreadCount; selectIndex++ {
		requestsComplete := make(chan interface{})
		requestIndex := selectIndex
		wait.Add(1)
		go func() {
			errors[requestIndex] = c.handleRequests(ctx, requestIndex)
			close(requestsComplete)
			wait.Done()
		}()

		selects[selectIndex] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(requestsComplete),
		}
	}

	expireComplete := make(chan interface{})
	expireIndex := selectIndex
	wait.Add(1)
	go func() {
		errors[expireIndex] = c.expireItems(ctx)
		close(expireComplete)
		wait.Done()
	}()

	selects[selectIndex] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(expireComplete),
	}
	selectIndex++

	interruptIndex := selectIndex
	selects[selectIndex] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(interrupt),
	}

	index, _, _ := reflect.Select(selects)
	if index == expireIndex {
		logger.Error(ctx, "Cache Expire Completed : %s", errors[index])
	} else if index < interruptIndex {
		logger.Error(ctx, "Cache Handle Requests %d Completed : %s", index, errors[index])
	}

	logger.Verbose(ctx, "Shutting down cache")

	// stop new requests from being added
	c.requestsLock.Lock()
	requests := c.requests
	c.requests = nil
	c.requestsLock.Unlock()

	// flush any existing requests
	flushRequests(requests, ErrShuttingDown)
	close(requests)

	c.expireLock.Lock()
	close(c.expirers)
	c.expirers = nil
	c.expireLock.Unlock()

	wait.Wait()

	return threads.CombineErrors(errors...)
}

func (w *CacheItem) addUser() {
	w.Lock()
	defer w.Unlock()

	w.users++
	w.lastUsed = time.Now()
}
