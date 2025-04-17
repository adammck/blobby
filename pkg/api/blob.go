package api

import (
	"fmt"
	"time"
)

type BlobMeta struct {
	MinKey  string    `bson:"min_key"`
	MaxKey  string    `bson:"max_key"`
	MinTime time.Time `bson:"min_time"`
	MaxTime time.Time `bson:"max_time"`
	Count   int       `bson:"count"`
	Size    int64     `bson:"size"`

	// Warning! Even though this is a time.Time, which has nanosecond precision
	// and a zone, when serialized to BSON, it's truncated into a UTC datetime
	// with only millisecond precision. Since the metadata store is currently
	// Mongo, round-tripping a Meta will result in different timestamps.
	//
	// See: https://bsonspec.org/spec.html
	Created time.Time `bson:"created"`
}

// Filename returns the filename of this sstable. It happens to be based on the
// creation time, but it should be considered opaque.
func (m *BlobMeta) Filename() string {
	return fmt.Sprintf("%d.sstable", m.Created.UnixMilli())
}
