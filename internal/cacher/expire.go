package cacher

import (
	"context"
	"time"
)

type ExpireItem struct {
	item *CacheItem

	lastUsed time.Time
}

func (c *Cache) runExpireItems(ctx context.Context, interrupt <-chan interface{},
	threadIndex int) error {

	expiration := c.expiration
	for {
		select {
		case expireItem, ok := <-c.expirers:
			if !ok {
				return nil
			}

			expireItem.item.Lock()

			if expireItem.item.lastUsed != expireItem.lastUsed {
				expireItem.item.Unlock()
				continue
			}

			if expireItem.item.users > 0 {
				expireItem.item.Unlock()
				continue
			}

			expiry := expireItem.lastUsed.Add(expiration)
			now := time.Now()
			if now.After(expiry) {
				expireItem.item.Unlock()
				if err := c.expireItem(ctx, threadIndex, expireItem); err != nil {
					return err
				}
				continue
			}

			expireItem.item.Unlock()

			// Wait for expiry
			wasInterrupted := false
			select {
			case <-time.After(expiry.Sub(now)):
			case <-interrupt:
				wasInterrupted = true
			}

			if err := c.expireItem(ctx, threadIndex, expireItem); err != nil {
				return err
			}

			if wasInterrupted {
				return nil
			}

		case <-interrupt:
			return nil
		}
	}
}

// finishExpiring processes any expire items until the items in use count goes to zero.
func (c *Cache) finishExpiring(ctx context.Context) error {
	select {
	case expireItem := <-c.expirers:
		if err := c.expireItem(ctx, -1, expireItem); err != nil {
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
		case expireItem, ok := <-c.expirers:
			if !ok {
				return nil
			}

			if err := c.expireItem(ctx, -1, expireItem); err != nil {
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

func (c *Cache) expireItem(ctx context.Context, threadIndex int, expireItem *ExpireItem) error {
	path := expireItem.item.value.Path()

	expireItem.item.Lock()

	if expireItem.item.users > 0 {
		expireItem.item.Unlock()
		return nil
	}

	if expireItem.lastUsed != expireItem.item.lastUsed {
		expireItem.item.Unlock()
		return nil
	}

	expireItem.item.Unlock()

	c.itemsLock.Lock()
	delete(c.items, path)
	c.itemsLock.Unlock()
	return nil
}
