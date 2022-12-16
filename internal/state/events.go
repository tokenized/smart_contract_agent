package state

import (
	"context"

	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/smart_contract_agent/pkg/scheduler"

	"github.com/pkg/errors"
)

const (
	eventPath = "events"
)

func ListEvents(ctx context.Context, store storage.StreamStorage) (scheduler.Events, error) {
	var result scheduler.Events
	if err := storage.StreamRead(ctx, store, eventPath, &result); err != nil {
		return nil, errors.Wrap(err, "read")
	}

	return result, nil
}

func SaveEvents(ctx context.Context, store storage.StreamStorage, events scheduler.Events) error {
	if err := storage.StreamWrite(ctx, store, eventPath, events); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}
