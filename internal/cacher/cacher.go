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

	fetchTimeout     time.Duration
	fetchTimeoutLock sync.Mutex

	requests     chan *Request
	requestsLock sync.Mutex

	fetchers    []*Fetcher
	fetcherLock sync.Mutex
	fetcherWait sync.WaitGroup

	newFetchers     chan *Fetcher
	newFetchersLock sync.Mutex

	items          map[string]*CacheItem
	itemsLock      sync.Mutex
	itemsAddLock   sync.Mutex
	itemsFetchLock sync.Mutex

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

func NewCache(store storage.StreamStorage, typ reflect.Type, fetcherCount, expireCount int,
	expiration, fetchTimeout time.Duration) (*Cache, error) {

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
		typ:          typ,
		store:        store,
		expiration:   expiration,
		fetchTimeout: fetchTimeout,
		requests:     make(chan *Request, 100),
		fetchers:     make([]*Fetcher, fetcherCount),
		newFetchers:  make(chan *Fetcher, fetcherCount),
		items:        make(map[string]*CacheItem),
		expirers:     make(chan *ExpireItem, expireCount),
	}, nil
}

func (c *Cache) Save(ctx context.Context, value CacheValue) error {
	path := value.Path()
	if err := storage.StreamWrite(ctx, c.store, path, value); err != nil {
		return errors.Wrapf(err, "write %s", path)
	}

	return nil
}

// Add adds an item if it isn't in the cache or storage yet. If it is already in the cache or
// storage then it returns the existing item and ignores the item parameter.
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

// Get gets an item from the cache and if it isn't in the cache it attempts to read it from storage.
// If it is found in storage it is added to the cache. If it isn't found in the cache or storage
// then (nil, nil) is returned. Release must be called after the item is no longer used to allow it
// to expire from the cache.
func (c *Cache) Get(ctx context.Context, path string) (CacheValue, error) {
	c.itemsLock.Lock()
	item, exists := c.items[path]
	if exists {
		item.addUser()
		c.itemsLock.Unlock()
		return item.value, nil
	}
	c.itemsLock.Unlock()

	response, err := c.createRequest(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}

	// Wait for the item to be retrieved.
	timeout := c.FetchTimeout()
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
		return nil, errors.Wrapf(ErrTimedOut, "%s", timeout)
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
	var wait sync.WaitGroup

	var handleRequestsError error
	requestsComplete := make(chan interface{})
	wait.Add(1)
	go func() {
		handleRequestsError = c.handleRequests(ctx)
		close(requestsComplete)
		wait.Done()
	}()

	var expireError error
	expireComplete := make(chan interface{})
	wait.Add(1)
	go func() {
		expireError = c.expireItems(ctx)
		close(expireComplete)
		wait.Done()
	}()

	select {
	case <-requestsComplete:
		logger.Error(ctx, "Cache Handle Requests Completed : %s", handleRequestsError)
	case <-expireComplete:
		logger.Error(ctx, "Cache Expire Completed : %s", expireError)
	case <-interrupt:
	}

	c.newFetchersLock.Lock()
	clearFetchers(c.newFetchers, ErrShuttingDown)
	close(c.newFetchers)
	c.newFetchers = nil
	c.newFetchersLock.Unlock()

	c.requestsLock.Lock()
	clearRequests(c.requests, ErrShuttingDown)
	close(c.requests)
	c.requests = nil
	c.requestsLock.Unlock()

	c.expireLock.Lock()
	close(c.expirers)
	c.expirers = nil
	c.expireLock.Unlock()

	c.fetcherWait.Wait()
	wait.Wait()

	return threads.CombineErrors(
		handleRequestsError,
		expireError,
	)
}

func (w *CacheItem) addUser() {
	w.Lock()
	defer w.Unlock()

	w.users++
	w.lastUsed = time.Now()
}
