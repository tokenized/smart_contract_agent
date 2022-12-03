package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/peer_channels"

	"github.com/pkg/errors"
)

const (
	respondersVersion = uint8(0)
	respondersPath    = "responders"
)

type ResponderCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type Responder struct {
	PeerChannels peer_channels.PeerChannels `bsor:"1" json:"peer_channels"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

func NewResponderCache(cache *cacher.Cache) (*ResponderCache, error) {
	typ := reflect.TypeOf(&Responder{})

	// Verify item value type is valid for a cache item.
	if typ.Kind() != reflect.Ptr {
		return nil, errors.New("Type must be a pointer")
	}

	itemValue := reflect.New(typ.Elem())
	if !itemValue.CanInterface() {
		return nil, errors.New("Type must support interface")
	}

	itemInterface := itemValue.Interface()
	if _, ok := itemInterface.(cacher.CacheValue); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &ResponderCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *ResponderCache) Add(ctx context.Context, lockingScript bitcoin.Script, txid bitcoin.Hash32,
	responders *Responder) (*Responder, error) {

	item, err := c.cacher.Add(ctx, c.typ, RespondersPath(lockingScript, txid), responders)
	if err != nil {
		return nil, errors.Wrap(err, "add")
	}

	return item.(*Responder), nil
}

func (c *ResponderCache) Get(ctx context.Context, lockingScript bitcoin.Script,
	txid bitcoin.Hash32) (*Responder, error) {

	item, err := c.cacher.Get(ctx, c.typ, RespondersPath(lockingScript, txid))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*Responder), nil
}

func (c *ResponderCache) Release(ctx context.Context, lockingScript bitcoin.Script,
	txid bitcoin.Hash32) {
	c.cacher.Release(ctx, RespondersPath(lockingScript, txid))
}

func RespondersPath(lockingScript bitcoin.Script, txid bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s/%s", CalculateContractHash(lockingScript), respondersPath, txid)
}

func (r *Responder) AddPeerChannel(peerChannel *peer_channels.PeerChannel) {
	s := peerChannel.String()
	for _, pc := range r.PeerChannels {
		if pc.String() == s {
			return // already have this peer channel
		}
	}

	r.PeerChannels = append(r.PeerChannels, peerChannel)
	r.isModified = true
}

func (r *Responder) RemovePeerChannels(peerChannels peer_channels.PeerChannels) {
	for _, peerChannel := range peerChannels {
		s := peerChannel.String()
		for i, pc := range r.PeerChannels {
			if pc.String() == s {
				r.PeerChannels = append(r.PeerChannels[:i], r.PeerChannels[i+1:]...)
				r.isModified = true
				break
			}
		}
	}
}

func (r *Responder) MarkModified() {
	r.isModified = true
}

func (r *Responder) ClearModified() {
	r.isModified = false
}

func (r *Responder) IsModified() bool {
	return r.isModified
}

func (r *Responder) CacheCopy() cacher.CacheValue {
	return r.Copy()
}

func (r *Responder) Copy() *Responder {
	result := &Responder{
		PeerChannels: make(peer_channels.PeerChannels, len(r.PeerChannels)),
	}

	for i, peerChannel := range r.PeerChannels {
		result.PeerChannels[i] = peerChannel
	}

	return result
}

func (r *Responder) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(r)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, respondersVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func (r *Responder) Deserialize(rdr io.Reader) error {
	var version uint8
	if err := binary.Read(rdr, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var size uint32
	if err := binary.Read(rdr, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(rdr, b); err != nil {
		return errors.Wrap(err, "read")
	}

	r.Lock()
	defer r.Unlock()
	if _, err := bsor.UnmarshalBinary(b, r); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}
