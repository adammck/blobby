package types

import (
	"time"
	"go.mongodb.org/mongo-driver/bson"
	"io"
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
