package types

import "time"

type Record struct {
	Key       string    `bson:"key"`
	Timestamp time.Time `bson:"ts"`
	Document  []byte    `bson:"doc"`
}
