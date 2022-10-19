package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/cacher"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/specification/dist/golang/actions"
	"github.com/tokenized/specification/dist/golang/protocol"

	"github.com/pkg/errors"
)

const (
	contractServicesVersion = uint8(0)
	contractServicesPath    = "services"
)

type ContractServicesCache struct {
	cacher *cacher.Cache
	typ    reflect.Type
}

type ContractServices struct {
	LockingScript bitcoin.Script             `bsor:"1" json:"locking_script"`
	Formation     *actions.ContractFormation `bsor:"-" json:"formation"`
	FormationTxID *bitcoin.Hash32            `bsor:"3" json:"formation_txid"`

	Services []*Service `bsor:"4" json:"service"`

	// FormationScript is only used by Serialize to save the Formation value in BSOR.
	FormationScript bitcoin.Script `bsor:"5" json:"formation_script"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

type Service struct {
	Type      uint32            `bsor:"1" json:"type"`
	URL       string            `bsor:"2" json:"url"`
	PublicKey bitcoin.PublicKey `bsor:"3" json:"public_key"`
}

func NewContractServicesCache(cache *cacher.Cache) (*ContractServicesCache, error) {
	typ := reflect.TypeOf(&ContractServices{})

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

	return &ContractServicesCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *ContractServicesCache) Update(ctx context.Context, lockingScript bitcoin.Script,
	formation *actions.ContractFormation, txid bitcoin.Hash32) error {

	path := ContractServicesPath(lockingScript)

	contractServices := &ContractServices{
		LockingScript: lockingScript,
		Formation:     formation,
		FormationTxID: &txid,
	}

	for i, service := range formation.Services {
		publicKey, err := bitcoin.PublicKeyFromBytes(service.PublicKey)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", lockingScript),
			}, "Invalid public key for contract service %d : %s", i, err)
			continue
		}

		contractServices.Services = append(contractServices.Services, &Service{
			Type:      service.Type,
			URL:       service.URL,
			PublicKey: publicKey,
		})
	}

	item, err := c.cacher.Add(ctx, c.typ, contractServices)
	if err != nil {
		return errors.Wrap(err, "add")
	}
	addedContractServices := item.(*ContractServices)

	if addedContractServices != contractServices {
		// Update existing contract services
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", lockingScript),
		}, "Updating existing contract services")

		addedContractServices.Lock()
		addedContractServices.Formation = formation
		addedContractServices.FormationTxID = &txid
		addedContractServices.Services = contractServices.Services
		addedContractServices.MarkModified()
		addedContractServices.Unlock()
	}

	c.cacher.Release(ctx, path)
	return nil
}

func (c *ContractServicesCache) Get(ctx context.Context,
	lockingScript bitcoin.Script) (*ContractServices, error) {

	item, err := c.cacher.Get(ctx, c.typ, ContractServicesPath(lockingScript))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*ContractServices), nil
}

func (c *ContractServicesCache) Release(ctx context.Context, lockingScript bitcoin.Script) {
	c.cacher.Release(ctx, ContractServicesPath(lockingScript))
}

func ContractServicesPath(lockingScript bitcoin.Script) string {
	return fmt.Sprintf("%s/%s", contractServicesPath, CalculateContractHash(lockingScript))
}

func (c *ContractServices) Path() string {
	return ContractServicesPath(c.LockingScript)
}

func (c *ContractServices) MarkModified() {
	c.isModified = true
}

func (c *ContractServices) ClearModified() {
	c.isModified = false
}

func (c *ContractServices) IsModified() bool {
	return c.isModified
}

func (c *ContractServices) Serialize(w io.Writer) error {
	if c.Formation != nil {
		script, err := protocol.Serialize(c.Formation, IsTest())
		if err != nil {
			return errors.Wrap(err, "serialize contract formation")
		}

		c.FormationScript = script
	}

	b, err := bsor.MarshalBinary(c)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, contractServicesVersion); err != nil {
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

func (c *ContractServices) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported version : %d", version)
	}

	var size uint32
	if err := binary.Read(r, endian, &size); err != nil {
		return errors.Wrap(err, "size")
	}

	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return errors.Wrap(err, "read")
	}

	c.Lock()
	defer c.Unlock()
	if _, err := bsor.UnmarshalBinary(b, c); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	if len(c.FormationScript) != 0 {
		action, err := protocol.Deserialize(c.FormationScript, IsTest())
		if err != nil {
			return errors.Wrap(err, "deserialize contract formation")
		}

		formation, ok := action.(*actions.ContractFormation)
		if !ok {
			return errors.New("FormationScript is wrong type")
		}

		c.Formation = formation
	}

	return nil
}
