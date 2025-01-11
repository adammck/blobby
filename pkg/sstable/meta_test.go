package sstable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetaFilename(t *testing.T) {
	meta := &Meta{
		Created: time.Unix(1234567890, 0),
	}

	assert.Equal(t, "1234567890.sstable", meta.Filename())
}
