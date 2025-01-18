package memtable

import (
	"fmt"
)

type NotFound struct {
	key string
}

func (e *NotFound) Error() string {
	return fmt.Sprintf("memtable: not found: %s", e.key)
}

func (e *NotFound) Is(err error) bool {
	_, ok := err.(*NotFound)
	return ok
}
