package cacher

import (
	"context"

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
	item, exists := c.items[request.path]
	if exists {
		item.addUser()
		c.itemsLock.Unlock()
		request.response <- item
		return nil
	}
	c.itemsLock.Unlock()

	// Fetch from storage.
	fetchResponse := make(chan interface{})
	if err := c.addFetcher(ctx, request.path, fetchResponse); err != nil {
		return errors.Wrap(err, "add fetcher")
	}

	c.fetcherWait.Add(1)
	go func() {
		c.waitForFetch(ctx, request, fetchResponse)
		c.fetcherWait.Done()
	}()

	return nil
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
