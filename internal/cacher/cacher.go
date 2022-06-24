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

	fetchers    []*Fetcher
	fetcherLock sync.Mutex
	fetcherWait sync.WaitGroup

	newFetchers     chan *Fetcher
	newFetchersLock sync.Mutex

	items          map[bitcoin.Hash32]*CacheItem
	itemsLock      sync.Mutex
	itemsAddLock   sync.Mutex
	itemsFetchLock sync.Mutex

	// TODO Add function to check if expires is full and being waited on, then don't wait for
	// expiration. This also limits the number of items in memory.
	expirers   chan *ExpireItem
	expireLock sync.Mutex
}

type Fetcher struct {
	store    storage.StreamStorage
	path     string
	value    CacheValue
	response chan<- interface{}
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

func NewCache(store storage.StreamStorage, typ reflect.Type, pathPrefix string,
	fetcherCount, expireCount int, timeout time.Duration) (*Cache, error) {

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
		typ:         typ,
		store:       store,
		timeout:     timeout,
		pathPrefix:  pathPrefix,
		requests:    make(chan *Request, 100),
		fetchers:    make([]*Fetcher, fetcherCount),
		newFetchers: make(chan *Fetcher, fetcherCount),
		items:       make(map[bitcoin.Hash32]*CacheItem),
		expirers:    make(chan *ExpireItem, expireCount),
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
	if c.expirers != nil {
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
	fetchResponse := make(chan interface{})
	if err := c.addFetcher(ctx, request.id, fetchResponse); err != nil {
		return errors.Wrap(err, "add fetcher")
	}

	c.fetcherWait.Add(1)
	go func() {
		c.waitForFetch(ctx, request, fetchResponse)
		c.fetcherWait.Done()
	}()

	return nil
}

func (c *Cache) waitForFetch(ctx context.Context, request *Request, response <-chan interface{}) {
	select {
	case fetchResponse := <-response:
		if fetchResponse == nil {
			request.response <- nil
			return
		}

		item, ok := fetchResponse.(*CacheItem)
		if !ok {
			request.response <- fetchResponse.(error)
		}

		c.itemsLock.Lock()
		existing, exists := c.items[request.id]
		if exists {
			// Already fetched in another thread
			existing.addUser()
			c.itemsLock.Unlock()

			request.response <- existing
			return
		}

		// Add to items
		c.items[request.id] = item
		c.itemsLock.Unlock()

		request.response <- item

	case <-time.After(c.Timeout()):
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.Stringer("id", request.id),
		}, "Fetch timed out")
		request.response <- ErrTimedOut
	}
}

func clearRequests(requests chan *Request, err error) {
	for {
		select {
		case request := <-requests:
			request.response <- err

		default:
			return
		}
	}
}

func newFetcher(store storage.StreamStorage, path string, value CacheValue,
	response chan<- interface{}) *Fetcher {
	return &Fetcher{
		store:    store,
		path:     path,
		value:    value,
		response: response,
	}
}

func (c *Cache) addFetcher(ctx context.Context, id bitcoin.Hash32,
	response chan<- interface{}) error {

	itemValue := reflect.New(c.typ.Elem())
	itemInterface := itemValue.Interface()
	fetcher := newFetcher(c.store, c.path(id), itemInterface.(CacheValue), response)

	c.newFetchersLock.Lock()
	defer c.newFetchersLock.Unlock()

	c.fetcherLock.Lock()
	for i, f := range c.fetchers {
		if f == nil {
			// There is an available fetcher slot so start it now
			c.fetchers[i] = fetcher
			c.fetcherLock.Unlock()

			c.fetcherWait.Add(1)
			go func() {
				defer c.removeFetcher(ctx, fetcher)
				fetcher.run(ctx)
				c.fetcherWait.Done()
			}()

			return nil
		}
	}
	c.fetcherLock.Unlock()

	if c.newFetchers != nil {
		c.newFetchers <- fetcher
		return nil
	}

	return ErrShuttingDown
}

func (c *Cache) removeFetcher(ctx context.Context, fetcher *Fetcher) {
	c.fetcherLock.Lock()
	defer c.fetcherLock.Unlock()

	for i, f := range c.fetchers {
		if fetcher == f {
			nextFetcher := c.nextFetcher()
			c.fetchers[i] = nextFetcher

			if nextFetcher != nil {
				c.fetcherWait.Add(1)
				go func() {
					defer c.removeFetcher(ctx, nextFetcher)
					nextFetcher.run(ctx)
					c.fetcherWait.Done()
				}()
			}

			return
		}
	}

	panic(fmt.Sprintf("Fetcher removed when not active : %s\n", fetcher.path))
}

func (c *Cache) nextFetcher() *Fetcher {
	c.newFetchersLock.Lock()
	defer c.newFetchersLock.Unlock()

	if c.newFetchers == nil {
		return nil
	}

	select {
	case fetcher := <-c.newFetchers:
		return fetcher

	default:
		return nil
	}
}

func clearFetchers(newFetchers chan *Fetcher, err error) {
	for {
		select {
		case fetcher := <-newFetchers:
			fetcher.response <- err

		default:
			return
		}
	}
}

func (f *Fetcher) run(ctx context.Context) {
	if err := storage.StreamRead(ctx, f.store, f.path, f.value); err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			f.response <- nil
			return
		}

		f.response <- errors.Wrapf(err, "read: %s", f.path)
		return
	}

	f.response <- &CacheItem{
		value:    f.value,
		users:    1,
		lastUsed: time.Now(),
	}
}

func (c *Cache) fetchValue(ctx context.Context, id bitcoin.Hash32) (CacheValue, error) {
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
