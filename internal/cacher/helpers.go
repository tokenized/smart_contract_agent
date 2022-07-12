package cacher

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

func writeString(w io.Writer, b []byte) error {
	if err := binary.Write(w, endian, uint32(len(b))); err != nil {
		return errors.Wrap(err, "size")
	}

	if _, err := w.Write(b); err != nil {
		return errors.Wrap(err, "value")
	}

	return nil
}

func readString(r io.Reader) ([]byte, error) {
	var s uint32
	if err := binary.Read(r, endian, &s); err != nil {
		return nil, errors.Wrap(err, "size")
	}

	b := make([]byte, s)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "value")
	}

	return b, nil
}

func appendStringIfDoesntExist(list []string, value string) []string {
	for _, v := range list {
		if v == value {
			return list // already contains value
		}
	}

	return append(list, value) // add value
}
