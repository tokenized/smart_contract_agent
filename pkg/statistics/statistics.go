package statistics

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokenized/pkg/bsor"
	"github.com/tokenized/pkg/cacher"
	"github.com/tokenized/smart_contract_agent/internal/state"

	"github.com/pkg/errors"
)

const (
	statisticsVersion = uint8(0)
	statisticsPath    = "statistics"
)

var (
	endian = binary.LittleEndian
)

// Statistics represent an accumulation of data for contracts and instruments that is cut off
// periodically and stored historically.
type Statistics struct {
	Actions []*Action `bsor:"1" json:"actions"`

	isModified atomic.Value
	sync.Mutex `bsor:"-"`
}

type Action struct {
	Code          string `bsor:"1" json:"code"`
	Count         uint64 `bsor:"2" json:"count"`
	RejectedCount uint64 `bsor:"3" json:"rejected_count"`
	InputCount    uint64 `bsor:"4" json:"input_count"`  // inputs relevant to the action (not tx funding)
	OutputCount   uint64 `bsor:"5" json:"output_count"` // outputs relevant to the action (not the paylod or tx funding)
}

type Update struct {
	Time           time.Time
	ContractHash   state.ContractHash
	InstrumentCode *state.InstrumentCode

	Code        string
	WasRejected bool
	InputCount  uint64
	OutputCount uint64
}

func (s *Statistics) Apply(update *Update) {
	s.Lock()
	defer s.Unlock()

	// Find action with matching code.
	var action *Action
	for _, act := range s.Actions {
		if act.Code == update.Code {
			action = act
			break
		}
	}

	if action == nil {
		// Create new action for code.
		action = &Action{
			Code: update.Code,
		}

		s.Actions = append(s.Actions, action)
	}

	action.Count++
	if update.WasRejected {
		action.RejectedCount++
	}

	action.InputCount += update.InputCount
	action.OutputCount += update.OutputCount

	s.MarkModified()
}

// Initializes any values that must be initialized.
func (s *Statistics) Initialize() {
	s.isModified.Store(false)
}

// IsModified returns true if the value has been marked modified, but does not clear the
// modified flag.
func (s *Statistics) IsModified() bool {
	if v := s.isModified.Load(); v != nil {
		return v.(bool)
	}

	return false
}

// MarkModified sets a modified flag so that a value will be saved to storage before being
// removed from the cache.
func (s *Statistics) MarkModified() {
	s.isModified.Store(true)
}

// GetModified returns true if the value has been modified and clears the modified flag.
func (s *Statistics) GetModified() bool {
	if v := s.isModified.Swap(false); v != nil {
		return v.(bool)
	}

	return false
}

func (s *Statistics) Serialize(w io.Writer) error {
	b, err := bsor.MarshalBinary(s)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := binary.Write(w, endian, statisticsVersion); err != nil {
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

func (s *Statistics) Deserialize(r io.Reader) error {
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

	s.Lock()
	defer s.Unlock()
	if _, err := bsor.UnmarshalBinary(b, s); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	return nil
}

// CacheCopy creates an independent copy of the value. IsModified should be initialized to false
// because the item will be new, so will be saved.
func (s *Statistics) CacheCopy() cacher.Value {
	result := &Statistics{
		Actions: make([]*Action, len(s.Actions)),
	}
	result.isModified.Store(true)

	for i, action := range s.Actions {
		a := action.Copy()
		result.Actions[i] = &a
	}

	return result
}

func (a *Action) Copy() Action {
	return Action{
		Code:          CopyString(a.Code),
		Count:         a.Count,
		RejectedCount: a.RejectedCount,
		InputCount:    a.InputCount,
		OutputCount:   a.OutputCount,
	}
}

func CopyString(s string) string {
	result := make([]byte, len(s))
	copy(result, s)
	return string(result)
}
