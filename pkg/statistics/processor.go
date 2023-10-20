package statistics

import (
	"context"
	"fmt"
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
	stat, err := p.Get(ctx, update)
	if err != nil {
		return errors.Wrap(err, "get")
	}
	defer p.Release(ctx, update)

	stat.Apply(update)
	return nil
}

func (p *Processor) Get(ctx context.Context, update *Update) (*Statistics, error) {
	path := update.Path()

	addStat := &Statistics{}
	addStat.Initialize()

	value, err := p.cache.Add(ctx, p.typ, path, addStat)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return value.(*Statistics), nil
}

func (p *Processor) Release(ctx context.Context, update *Update) {
	p.cache.Release(ctx, update.Path())
}

func (u *Update) Path() string {
	if u.InstrumentCode == nil {
		return fmt.Sprintf("%s/%s/%s", statisticsPath, u.ContractHash, u.Time.Format(dateFormat))
	}

	return fmt.Sprintf("%s/%s/%s/%s", statisticsPath, u.ContractHash, u.InstrumentCode,
		u.Time.Format(dateFormat))
}
