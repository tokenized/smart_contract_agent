package cacher

import (
	"context"
	"time"

	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

func (c *Cache) runSaveItems(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		select {
		case item, ok := <-c.saving:
			if !ok {
				return nil
			}

			if err := c.saveItem(ctx, item); err != nil {
				return errors.Wrap(err, "save item")
			}

		case <-interrupt:
			return nil
		}
	}
}

// finishSaving processes any saves until the items in use count goes to zero.
func (c *Cache) finishSaving(ctx context.Context) error {
	select {
	case item := <-c.saving:
		if err := c.saveItem(ctx, item); err != nil {
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
		case item, ok := <-c.saving:
			if !ok {
				return nil
			}

			if err := c.saveItem(ctx, item); err != nil {
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

func (c *Cache) saveItem(ctx context.Context, item *CacheItem) error {
	item.Lock()
	item.value.Lock()
	path := item.value.Path()

	if !item.isNew && !item.value.IsModified() {
		item.value.Unlock()
		c.releaseItem(ctx, item, path)
		item.Unlock()
		return nil
	}

	if err := storage.StreamWrite(ctx, c.store, path, item.value); err != nil {
		item.value.Unlock()
		item.Unlock()
		return errors.Wrapf(err, "write %s", path)
	}

	item.isNew = false
	item.value.ClearModified()
	item.value.Unlock()

	c.releaseItem(ctx, item, path)
	item.Unlock()
	return nil
}
