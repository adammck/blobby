package sstable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetaFilename(t *testing.T) {
	meta := &Meta{
		Created: time.Unix(1234567890, 1234999999),
	}

	// truncated (not rounded) to milliseconds.
	assert.Equal(t, "1234567891234.sstable", meta.Filename())
}
