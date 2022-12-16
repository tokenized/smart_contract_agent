package scheduler

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/bsor"

	"github.com/pkg/errors"
)

const (
	EventTypeInvalid            = EventType(0)
	EventTypeVoteCutOff         = EventType(1)
	EventTypeTransferExpiration = EventType(2)

	eventVersion = uint8(0)
)

var (
	endian = binary.LittleEndian
)

type Event struct {
	Type                  EventType      `bsor:"1" json:"type"`
	Start                 uint64         `bsor:"2" json:"start"`
	ContractLockingScript bitcoin.Script `bsor:"3" json:"contract_locking_script"`
	ID                    bitcoin.Hash32 `bsor:"4" json:"id"`
}

type Events []*Event

type EventType uint8

func (es Events) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(es)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, eventVersion); err != nil {
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

func (es *Events) Deserialize(r io.Reader) error {
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

	if _, err := bsor.UnmarshalBinary(b, es); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}

func (v EventType) MarshalText() ([]byte, error) {
	s := v.String()
	if len(s) == 0 {
		return nil, fmt.Errorf("Unknown EventType value \"%d\"", uint8(v))
	}

	return []byte(s), nil
}

func (v *EventType) UnmarshalText(text []byte) error {
	return v.SetString(string(text))
}

func (v *EventType) SetString(s string) error {
	switch s {
	case "vote_cutoff":
		*v = EventTypeVoteCutOff
	case "transfer_expiration":
		*v = EventTypeTransferExpiration
	default:
		*v = EventTypeInvalid
		return fmt.Errorf("Unknown EventType value \"%s\"", s)
	}

	return nil
}

func (v EventType) String() string {
	switch v {
	case EventTypeVoteCutOff:
		return "vote_cutoff"
	case EventTypeTransferExpiration:
		return "transfer_expiration"
	default:
		return ""
	}
}
