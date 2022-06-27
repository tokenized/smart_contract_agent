package cacher

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

type Fetcher struct {
	store    storage.StreamStorage
	path     string
	value    CacheValue
	response chan<- interface{}
}

func (c *Cache) FetchTimeout() time.Duration {
	c.fetchTimeoutLock.Lock()
	defer c.fetchTimeoutLock.Unlock()

	return c.fetchTimeout
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
		existing, exists := c.items[request.path]
		if exists {
			// Already fetched in another thread
			existing.addUser()
			c.itemsLock.Unlock()

			request.response <- existing
			return
		}

		// Add to items
		c.items[request.path] = item
		c.itemsLock.Unlock()

		request.response <- item

	case <-time.After(c.FetchTimeout()):
		logger.ErrorWithFields(ctx, []logger.Field{
			logger.String("path", request.path),
		}, "Fetch timed out")
		request.response <- ErrTimedOut
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

func (c *Cache) addFetcher(ctx context.Context, path string,
	response chan<- interface{}) error {

	itemValue := reflect.New(c.typ.Elem())
	itemInterface := itemValue.Interface()
	fetcher := newFetcher(c.store, path, itemInterface.(CacheValue), response)

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

func (c *Cache) fetchValue(ctx context.Context, path string) (CacheValue, error) {
	itemValue := reflect.New(c.typ.Elem())
	itemInterface := itemValue.Interface()
	item := itemInterface.(CacheValue)
	if err := storage.StreamRead(ctx, c.store, path, item); err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "read: %s", path)
	}

	return item, nil
}
