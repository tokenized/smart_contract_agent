package cacher

import (
	"context"
	"reflect"
	"time"

	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

type Request struct {
	path     string
	response chan<- interface{}
}

func (c *Cache) createRequest(ctx context.Context, path string) (<-chan interface{}, error) {
	// Create a request to get the tx.
	response := make(chan interface{}, 1)
	request := &Request{
		path:     path,
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

func (c *Cache) handleRequests(ctx context.Context, threadIndex int) error {
	for request := range c.requests {
		if err := c.handleRequest(ctx, threadIndex, request); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cache) handleRequest(ctx context.Context, threadIndex int, request *Request) error {
	c.itemsLock.Lock()
	item, exists := c.items[request.path]
	if exists {
		item.addUser()
		c.itemsLock.Unlock()
		request.response <- item
		return nil
	}
	c.itemsLock.Unlock()

	value, err := c.fetchValue(ctx, request.path)
	if err != nil {
		return errors.Wrapf(err, "read: %s", request.path)
	}

	if value == nil { // not found in storage
		request.response <- nil
		return nil
	}

	c.itemsLock.Lock()
	item, exists = c.items[request.path]
	if exists {
		// Already fetched in another thread
		item.addUser()
		c.itemsLock.Unlock()

		request.response <- item
		return nil
	}

	item = &CacheItem{
		value:    value,
		users:    1,
		lastUsed: time.Now(),
	}

	// Add to items
	c.items[request.path] = item
	c.itemsLock.Unlock()

	request.response <- item
	return nil
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

func flushRequests(requests chan *Request, err error) {
	for {
		select {
		case request := <-requests:
			request.response <- err

		default:
			return
		}
	}
}
