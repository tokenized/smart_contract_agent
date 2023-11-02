package statistics

import (
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/tokenized/pkg/cacher"

	"github.com/pkg/errors"
)

const (
	dateFormat = "2006-01" // Just year and month so it cuts off each month
)

var (
	ErrTimeout = errors.New("Timeout")
)

// A multi-threaded processor to update statistics outside of the request handling threads.
type Processor struct {
	cache cacher.Cacher
	typ   reflect.Type

	addTimeout atomic.Value
	updates    chan *Update
}

func NewProcessor(cache cacher.Cacher, channelSize int,
	addTimeout time.Duration) (*Processor, error) {

	typ := reflect.TypeOf(&Statistics{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.Value); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	result := &Processor{
		cache:   cache,
		typ:     typ,
		updates: make(chan *Update, channelSize),
	}
	result.addTimeout.Store(addTimeout)

	return result, nil
}

func (p *Processor) Add(ctx context.Context, update *Update) error {
	timeout := p.addTimeout.Load().(time.Duration)

	select {
	case p.updates <- update:
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	}
}

func (p *Processor) Run(ctx context.Context, interrupt <-chan interface{}) error {
	for {
		select {
		case update := <-p.updates:
			if err := p.Process(ctx, update); err != nil {
				return errors.Wrap(err, "process")
			}

		case <-interrupt:
			return nil
		}
	}
}

func (p *Processor) Process(ctx context.Context, update *Update) error {
	// Get statistics relating to the update.
	path := update.Path()
	stat, err := Add(ctx, p.cache, p.typ, path)
	if err != nil {
		return errors.Wrap(err, "get")
	}
	defer p.cache.Release(ctx, path)

	// Add the update's data to the statistics.
	stat.Apply(update)
	return nil
}
