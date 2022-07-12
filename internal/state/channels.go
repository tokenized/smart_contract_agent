package state

import (
	"context"

	"github.com/tokenized/channels"
)

type Channel struct {
}

func (c *Channel) SendMessage(ctx context.Context, msg channels.Writer) error {

	// script, err := channels.Wrap(msg, key bitcoin.Key, hash bitcoin.Hash32, messageID uint64, nil)

	return nil
}
