package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type Record struct {
	Key       string    `bson:"key"`
	Timestamp time.Time `bson:"ts"`
	Document  []byte    `bson:"doc,omitempty"`
	Tombstone bool      `bson:"tombstone"`
}

func (r *Record) Write(out io.Writer) (int, error) {
	b, err := bson.Marshal(r)
	if err != nil {
		return 0, err
	}

	return out.Write(b)
}

func Read(r io.Reader) (*Record, error) {
	b, err := readOne(r)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	rec := &Record{}
	if err := bson.Unmarshal(b, rec); err != nil {
		return nil, err
	}

	// If it's a tombstone record, ensure Document is nil
	if rec.Tombstone && len(rec.Document) == 0 {
		rec.Document = nil
	}

	return rec, nil
}

func readOne(r io.Reader) ([]byte, error) {
	// see: https://bsonspec.org/spec.html

	var sizeBytes [4]byte
	_, err := io.ReadFull(r, sizeBytes[:])
	if err != nil {
		// might be io.EOF; that's okay.
		return nil, err
	}

	size := int(binary.LittleEndian.Uint32(sizeBytes[:]))
	if size < 5 {
		return nil, fmt.Errorf("invalid BSON document length: want=5, got=%d", size)
	}

	docBytes := make([]byte, size)
	copy(docBytes[0:4], sizeBytes[:])
	_, err = io.ReadFull(r, docBytes[4:])
	if err != nil {
		return nil, fmt.Errorf("ReadFull(doc): %w", err)
	}

	return docBytes, nil
}
