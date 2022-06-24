package cacher

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
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
	ID() bitcoin.Hash32
	Serialize(w io.Writer) error
	Deserialize(r io.Reader) error
}

type Cache struct {
	typ reflect.Type

	store       storage.StreamStorage
	timeout     time.Duration
	timeoutLock sync.Mutex

	pathPrefix string
	pathLock   sync.Mutex

	requests     chan *Request
	requestsLock sync.Mutex

	items          map[bitcoin.Hash32]*CacheItem
	itemsLock      sync.Mutex
	itemsAddLock   sync.Mutex
	itemsFetchLock sync.Mutex

	// TODO Add function to check if expires is full and being waited on, then don't wait for
	// expiration. This also limits the number of items in memory.
	expirers   chan *ExpireItem
	expireLock sync.Mutex

	shuttingDown     bool
	shuttingDownLock sync.Mutex
}

type CacheItem struct {
	value CacheValue

	users    uint
	lastUsed time.Time
	sync.Mutex
}

type ExpireItem struct {
	item *CacheItem

	lastUsed time.Time
}

type Request struct {
	id       bitcoin.Hash32
	response chan<- interface{}
}

func NewCache(store storage.StreamStorage, typ reflect.Type, pathPrefix string, expireCount int,
	timeout time.Duration) (*Cache, error) {

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
		typ:        typ,
		store:      store,
		timeout:    timeout,
		pathPrefix: pathPrefix,
		requests:   make(chan *Request, 100),
		items:      make(map[bitcoin.Hash32]*CacheItem),
		expirers:   make(chan *ExpireItem, expireCount),
	}, nil
}

func (c *Cache) Timeout() time.Duration {
	c.timeoutLock.Lock()
	defer c.timeoutLock.Unlock()

	return c.timeout
}

func (c *Cache) Save(ctx context.Context, value CacheValue) error {
	path := c.path(value.ID())
	if err := storage.StreamWrite(ctx, c.store, path, value); err != nil {
		return errors.Wrapf(err, "write %s", path)
	}

	return nil
}

func (c *Cache) Add(ctx context.Context, value CacheValue) (CacheValue, error) {
	id := value.ID()

	// TODO This might be slow to add items since it requires a hit to storage to ensure it isn't
	// already there. But that might be unavoidable. --ce
	c.itemsAddLock.Lock()
	defer c.itemsAddLock.Unlock()

	gotItem, err := c.Get(ctx, id)
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
	c.items[id] = newItem
	c.itemsLock.Unlock()

	if err := c.Save(ctx, value); err != nil {
		return nil, errors.Wrap(err, "save")
	}

	return value, nil
}

func (c *Cache) Get(ctx context.Context, id bitcoin.Hash32) (CacheValue, error) {
	c.itemsLock.Lock()
	item, exists := c.items[id]
	if exists {
		item.addUser()
		c.itemsLock.Unlock()
		return item.value, nil
	}
	c.itemsLock.Unlock()

	response, err := c.createRequest(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "request")
	}

	// Wait for the item to be retrieved.
	timeout := c.Timeout()
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

func (c *Cache) Release(ctx context.Context, id bitcoin.Hash32) {
	c.itemsLock.Lock()
	item, exists := c.items[id]
	if !exists {
		c.itemsLock.Unlock()
		panic(fmt.Sprintf("Item released when not in cache: %s\n", id.String()))
	}
	item.Lock()
	c.itemsLock.Unlock()

	if item.users == 0 {
		item.Unlock()
		panic(fmt.Sprintf("Item released with no users: %s\n", id.String()))
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
	if !c.IsShuttingDown() {
		c.expirers <- &ExpireItem{
			item:     item,
			lastUsed: now,
		}
	}
	c.expireLock.Unlock()
}

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

	c.shuttingDownLock.Lock()
	c.shuttingDown = true
	c.shuttingDownLock.Unlock()

	c.requestsLock.Lock()
	close(c.requests)
	c.requestsLock.Unlock()
	c.expireLock.Lock()
	close(c.expirers)
	c.expireLock.Unlock()
	wait.Wait()

	return threads.CombineErrors(
		handleRequestsError,
		expireError,
	)
}

func (c *Cache) IsShuttingDown() bool {
	c.shuttingDownLock.Lock()
	defer c.shuttingDownLock.Unlock()

	return c.shuttingDown
}

func (c *Cache) expireItems(ctx context.Context) error {
	timeout := c.Timeout()
	for expirer := range c.expirers {
		expirer.item.Lock()

		if expirer.item.lastUsed != expirer.lastUsed {
			expirer.item.Unlock()
			continue
		}

		if expirer.item.users > 0 {
			expirer.item.Unlock()
			continue
		}

		expiry := expirer.lastUsed.Add(timeout)
		now := time.Now()
		if now.After(expiry) {
			expirer.item.Unlock()
			c.expireItem(ctx, expirer)
			continue
		}

		expirer.item.Unlock()

		// Wait for expiry
		time.Sleep(expiry.Sub(now))

		c.expireItem(ctx, expirer)
	}

	return nil
}

func (c *Cache) expireItem(ctx context.Context, expirer *ExpireItem) {
	id := expirer.item.value.ID()

	expirer.item.Lock()

	if expirer.item.users > 0 {
		expirer.item.Unlock()
		return
	}

	if expirer.lastUsed != expirer.item.lastUsed {
		expirer.item.Unlock()
		return
	}

	expirer.item.Unlock()

	c.itemsLock.Lock()
	delete(c.items, id)
	c.itemsLock.Unlock()
}

func (c *Cache) createRequest(ctx context.Context, id bitcoin.Hash32) (<-chan interface{}, error) {
	// Create a request to get the tx.
	response := make(chan interface{}, 1)
	request := &Request{
		id:       id,
		response: response,
	}

	c.requestsLock.Lock()
	if c.requests == nil {
		c.requestsLock.Unlock()
		return nil, ErrShuttingDown
	}
	c.requests <- request
	c.requestsLock.Unlock()

	return response, nil
}

func (c *Cache) handleRequests(ctx context.Context) error {
	var returnErr error
	for request := range c.requests {
		if err := c.handleRequest(ctx, request); err != nil {
			returnErr = err
			break
		}
	}

	clearRequests(c.requests, ErrShuttingDown)
	return returnErr
}

func (c *Cache) handleRequest(ctx context.Context, request *Request) error {
	// Check if item already exists. It could have been in a pending request when originally
	// fetched.
	c.itemsFetchLock.Lock()
	defer c.itemsFetchLock.Unlock()

	c.itemsLock.Lock()
	item, exists := c.items[request.id]
	if exists {
		item.addUser()
		c.itemsLock.Unlock()
		request.response <- item
		return nil
	}
	c.itemsLock.Unlock()

	// Fetch from storage.
	value, err := c.fetchItem(ctx, request.id)
	if err != nil {
		request.response <- err
		return errors.Wrap(err, "fetch")
	}

	if value == nil {
		request.response <- nil
		return nil
	}

	item = &CacheItem{
		value:    value,
		users:    1,
		lastUsed: time.Now(),
	}

	c.itemsLock.Lock()
	c.items[request.id] = item
	c.itemsLock.Unlock()

	request.response <- item
	return nil
}

func clearRequests(requests chan *Request, err error) {
	for request := range requests {
		request.response <- err
	}
}

func (c *Cache) fetchItem(ctx context.Context, id bitcoin.Hash32) (CacheValue, error) {
	itemValue := reflect.New(c.typ.Elem())
	itemInterface := itemValue.Interface()
	item := itemInterface.(CacheValue)
	path := c.path(id)
	if err := storage.StreamRead(ctx, c.store, path, item); err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "read: %s", path)
	}

	return item, nil
}

func (c *Cache) path(id bitcoin.Hash32) string {
	c.pathLock.Lock()
	defer c.pathLock.Unlock()

	return fmt.Sprintf("%s/%s", c.pathPrefix, id)
}

func (w *CacheItem) addUser() {
	w.Lock()
	defer w.Unlock()

	w.users++
	w.lastUsed = time.Now()
}
