package sstable

import (
	"fmt"
	"time"
)

type Meta struct {
	MinKey  string    `bson:"min_key"`
	MaxKey  string    `bson:"max_key"`
	MinTime time.Time `bson:"min_time"`
	MaxTime time.Time `bson:"max_time"`
	Count   int       `bson:"count"`
	Size    int       `bson:"size"`
	Created time.Time `bson:"created"`
}

func (m *Meta) Filename() string {
	return fmt.Sprintf("%d.sstable", m.Created.Unix())
}
