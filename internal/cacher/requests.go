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
	typ      reflect.Type
	response chan<- interface{}
}

func (c *Cache) createRequest(ctx context.Context, path string,
	typ reflect.Type) (<-chan interface{}, error) {

	// Create a request to get the tx.
	response := make(chan interface{}, 1)
	request := &Request{
		path:     path,
		typ:      typ,
		response: response,
	}

	c.requests <- request
	return response, nil
}

func (c *Cache) runHandleRequests(ctx context.Context, interrupt <-chan interface{},
	threadIndex int) error {

	for {
		select {
		case request, ok := <-c.requests:
			if !ok {
				return nil
			}

			if err := c.handleRequest(ctx, threadIndex, request); err != nil {
				return err
			}

		case <-interrupt:
			return nil
		}
	}
}

// finishHandlingRequests handles any requests until the items in use count goes to zero.
func (c *Cache) finishHandlingRequests(ctx context.Context) error {
	select {
	case request := <-c.requests:
		if err := c.handleRequest(ctx, -1, request); err != nil {
			return err
		}

	default:
		c.itemsUseCountLock.Lock()
		itemsUseCount := c.itemsUseCount
		c.itemsUseCountLock.Unlock()

		if itemsUseCount == 0 {
			return nil
		}
	}

	for {
		select {
		case request, ok := <-c.requests:
			if !ok {
				return nil
			}

			if err := c.handleRequest(ctx, -1, request); err != nil {
				return err
			}

		case <-time.After(100 * time.Millisecond):
			c.itemsUseCountLock.Lock()
			itemsUseCount := c.itemsUseCount
			c.itemsUseCountLock.Unlock()

			if itemsUseCount == 0 {
				return nil
			}
		}
	}
}

func (c *Cache) handleRequest(ctx context.Context, threadIndex int, request *Request) error {
	c.itemsLock.Lock()
	item, exists := c.items[request.path]
	if exists {
		c.addUser(item)
		c.itemsLock.Unlock()
		request.response <- item
		return nil
	}
	c.itemsLock.Unlock()

	value, err := c.fetchValue(ctx, request.path, request.typ)
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
		c.addUser(item)
		c.itemsLock.Unlock()

		request.response <- item
		return nil
	}

	item = &CacheItem{
		value: value,
	}
	c.addUser(item)

	// Add to items
	c.items[request.path] = item
	c.itemsLock.Unlock()

	request.response <- item
	return nil
}

func (c *Cache) fetchValue(ctx context.Context, path string, typ reflect.Type) (CacheValue, error) {
	itemValue := reflect.New(typ.Elem())
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
