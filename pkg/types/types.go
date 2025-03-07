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
	Document  []byte    `bson:"doc"`
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

	return rec, nil
}

func readOne(r io.Reader) ([]byte, error) {
	// see: https://bsonspec.org/spec.html

	var sizeBytes [4]byte
	if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}

	size := int(binary.LittleEndian.Uint32(sizeBytes[:]))
	if size < 5 {
		return nil, fmt.Errorf("invalid BSON document length: want=5, got=%d", size)
	}

	docBytes := make([]byte, size)
	copy(docBytes[0:4], sizeBytes[:])
	if _, err := io.ReadFull(r, docBytes[4:]); err != nil {
		return nil, err
	}

	return docBytes, nil
}
