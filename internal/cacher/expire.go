package cacher

import (
	"context"
	"time"
)

type ExpireItem struct {
	item *CacheItem

	lastUsed time.Time
}

func (c *Cache) Expiration() time.Duration {
	c.expirationLock.Lock()
	defer c.expirationLock.Unlock()

	return c.expiration
}

func (c *Cache) expireItems(ctx context.Context) error {
	expiration := c.Expiration()
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

		expiry := expirer.lastUsed.Add(expiration)
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
	path := expirer.item.value.Path()

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
	delete(c.items, path)
	c.itemsLock.Unlock()
}
