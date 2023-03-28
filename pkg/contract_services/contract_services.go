package contract_services

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"
	ci "github.com/tokenized/pkg/cacher"
	"github.com/tokenized/smart_contract_agent/internal/state"
	"github.com/tokenized/specification/dist/golang/actions"

	"github.com/pkg/errors"
)

const (
	contractServicesVersion = uint8(0)
	contractServicesPath    = "services"
)

var (
	endian = binary.LittleEndian
)

type ContractServicesCache struct {
	cacher ci.Cacher
	typ    reflect.Type
}

type ContractServices struct {
	Formation     *actions.ContractFormation `bsor:"1" json:"formation"`
	FormationTxID *bitcoin.Hash32            `bsor:"2" json:"formation_txid"`

	Services []*Service `bsor:"3" json:"service"`

	isModified bool
	sync.Mutex `bsor:"-"`
}

type Service struct {
	Type      uint32            `bsor:"1" json:"type"`
	URL       string            `bsor:"2" json:"url"`
	PublicKey bitcoin.PublicKey `bsor:"3" json:"public_key"`
}

func NewContractServicesCache(cache ci.Cacher) (*ContractServicesCache, error) {
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
	if _, ok := itemInterface.(ci.Value); !ok {
		return nil, errors.New("Type must implement CacheValue")
	}

	return &ContractServicesCache{
		cacher: cache,
		typ:    typ,
	}, nil
}

func (c *ContractServicesCache) Update(ctx context.Context, contractLockingScript bitcoin.Script,
	formation *actions.ContractFormation, txid bitcoin.Hash32) error {

	contractHash := state.CalculateContractHash(contractLockingScript)

	path := ContractServicesPath(contractHash)

	if len(formation.Services) == 0 { // Clear any previous services
		item, err := c.cacher.Get(ctx, c.typ, path)
		if err != nil {
			return errors.Wrap(err, "add")
		}

		if item == nil {
			return nil // no previous services
		}

		contractServices := item.(*ContractServices)
		contractServices.Lock()
		if len(contractServices.Services) > 0 {
			contractServices.Services = nil
			contractServices.isModified = true
		}
		contractServices.Unlock()

		c.cacher.Release(ctx, path)
	}

	contractServices := &ContractServices{
		Formation:     formation,
		FormationTxID: &txid,
	}

	for i, service := range formation.Services {
		publicKey, err := bitcoin.PublicKeyFromBytes(service.PublicKey)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", contractLockingScript),
			}, "Invalid public key for contract service %d : %s", i, err)
			continue
		}

		contractServices.Services = append(contractServices.Services, &Service{
			Type:      service.Type,
			URL:       service.URL,
			PublicKey: publicKey,
		})
	}

	item, err := c.cacher.Add(ctx, c.typ, path, contractServices)
	if err != nil {
		return errors.Wrap(err, "add")
	}
	addedContractServices := item.(*ContractServices)

	if addedContractServices != contractServices {
		// Update existing contract services
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractLockingScript),
		}, "Updating existing contract services")

		addedContractServices.Lock()
		addedContractServices.Formation = formation
		addedContractServices.FormationTxID = &txid
		addedContractServices.Services = contractServices.Services
		addedContractServices.MarkModified()
		addedContractServices.Unlock()
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", contractLockingScript),
		}, "New contract services")
	}

	c.cacher.Release(ctx, path)
	return nil
}

func (c *ContractServicesCache) Get(ctx context.Context,
	contractLockingScript bitcoin.Script) (*ContractServices, error) {

	contractHash := state.CalculateContractHash(contractLockingScript)

	item, err := c.cacher.Get(ctx, c.typ, ContractServicesPath(contractHash))
	if err != nil {
		return nil, errors.Wrap(err, "get")
	}

	if item == nil {
		return nil, nil
	}

	return item.(*ContractServices), nil
}

func (c *ContractServicesCache) Release(ctx context.Context, contractLockingScript bitcoin.Script) {
	c.cacher.Release(ctx, ContractServicesPath(state.CalculateContractHash(contractLockingScript)))
}

func ContractServicesPath(contractHash state.ContractHash) string {
	return fmt.Sprintf("%s/%s", contractServicesPath, contractHash)
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

func (s *ContractServices) CacheCopy() ci.Value {
	result := &ContractServices{
		Services: make([]*Service, len(s.Services)),
	}

	if s.Formation != nil {
		result.Formation = s.Formation.Copy()
	}

	if s.FormationTxID != nil {
		result.FormationTxID = &bitcoin.Hash32{}
		copy(result.FormationTxID[:], s.FormationTxID[:])
	}

	for i, service := range s.Services {
		result.Services[i] = &Service{
			Type:      service.Type,
			URL:       service.URL,
			PublicKey: service.PublicKey,
		}
	}

	return result
}

func (c *ContractServices) Serialize(w io.Writer) error {
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

	return nil
}
