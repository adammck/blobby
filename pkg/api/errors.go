package api

import (
	"fmt"
)

type IndexNotFound struct {
	Filename string
}

func (e *IndexNotFound) Error() string {
	return fmt.Sprintf("index not found: %s", e.Filename)
}

func (e *IndexNotFound) Is(err error) bool {
	_, ok := err.(*IndexNotFound)
	return ok
}
