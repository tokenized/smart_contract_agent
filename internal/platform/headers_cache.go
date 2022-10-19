package platform

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/pkg/bitcoin"
)

type BlockHeaders interface {
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)
}

type HeadersCache struct {
	cache map[int]*bitcoin.Hash32

	headers BlockHeaders
}

func NewHeadersCache(headers BlockHeaders) *HeadersCache {
	return &HeadersCache{
		cache:   make(map[int]*bitcoin.Hash32),
		headers: headers,
	}
}

func (h *HeadersCache) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	hash, exists := h.cache[height]
	if exists {
		return hash, nil
	}

	fetchedHash, err := h.headers.BlockHash(ctx, height)
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	h.cache[height] = fetchedHash
	return fetchedHash, nil
}
