package headers

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	spyNodeClient "github.com/tokenized/spynode/pkg/client"

	"github.com/pkg/errors"
)

// GetBlockHeaders is the interface used by spynode client to provide block headers.
type GetBlockHeaders interface {
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)
	GetHeaders(context.Context, int, int) (*spyNodeClient.Headers, error)
}

// BlockHeaders is the interface implemented by this package to provide block headers.
type BlockHeaders interface {
	BlockHash(context.Context, int) (*bitcoin.Hash32, error)
	GetHeader(context.Context, int) (*wire.BlockHeader, error)
}

type Headers struct {
	headers GetBlockHeaders
}

type HeadersCache struct {
	hashCache   map[int]*bitcoin.Hash32
	headerCache map[int]*wire.BlockHeader

	headers BlockHeaders
}

func NewHeaders(headers GetBlockHeaders) *Headers {
	return &Headers{
		headers: headers,
	}
}

func NewHeadersCache(headers BlockHeaders) *HeadersCache {
	return &HeadersCache{
		hashCache: make(map[int]*bitcoin.Hash32),
		headers:   headers,
	}
}

func (h *Headers) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	return h.headers.BlockHash(ctx, height)
}

func (h *Headers) GetHeader(ctx context.Context, height int) (*wire.BlockHeader, error) {
	headers, err := h.headers.GetHeaders(ctx, height, 1)
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	if len(headers.Headers) == 0 {
		return nil, nil
	}

	return headers.Headers[0], nil
}

func (h *HeadersCache) BlockHash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	hash, exists := h.hashCache[height]
	if exists {
		return hash, nil
	}

	fetchedHash, err := h.headers.BlockHash(ctx, height)
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	h.hashCache[height] = fetchedHash
	return fetchedHash, nil
}

func (h *HeadersCache) GetHeader(ctx context.Context, height int) (*wire.BlockHeader, error) {
	hash, exists := h.headerCache[height]
	if exists {
		return hash, nil
	}

	fetchedHeader, err := h.headers.GetHeader(ctx, height)
	if err != nil {
		return nil, errors.Wrap(err, "fetch")
	}

	h.headerCache[height] = fetchedHeader
	return fetchedHeader, nil
}
