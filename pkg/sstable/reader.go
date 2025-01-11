package sstable

import (
	"errors"
	"fmt"
	"io"

	"github.com/adammck/archive/pkg/types"
	"go.mongodb.org/mongo-driver/bson"
)

type Reader struct {
	r   io.Reader
	dec *bson.Decoder
}

func NewReader(r io.Reader) (*Reader, error) {
	magic := make([]byte, len(magicBytes))
	if _, err := io.ReadFull(r, magic); err != nil {
		return nil, fmt.Errorf("read magic bytes: %w", err)
	}
	if string(magic) != magicBytes {
		return nil, fmt.Errorf("invalid sstable format")
	}

	return &Reader{
		r: r,
	}, nil
}

func (r *Reader) Next() (*types.Record, error) {
	b, err := nextDoc(r.r)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("nextDoc: %w", err)
	}

	rec := types.Record{}
	if err := bson.Unmarshal(b, &rec); err != nil {
		return nil, err
	}

	return &rec, nil
}

func nextDoc(r io.Reader) ([]byte, error) {
	// four byte length prefix
	var lengthBytes [4]byte
	if _, err := io.ReadFull(r, lengthBytes[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}

	// Convert to length (little endian)
	length := int(lengthBytes[0]) | int(lengthBytes[1])<<8 | int(lengthBytes[2])<<16 | int(lengthBytes[3])<<24
	if length < 5 {
		return nil, errors.New("invalid BSON document length")
	}

	// buffer for full doc
	doc := make([]byte, length)
	copy(doc[0:4], lengthBytes[:])

	// read the rest of the doc
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return nil, err
	}

	return doc, nil
}
