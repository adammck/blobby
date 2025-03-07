package sstable

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReader(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))
	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestNewReaderInvalidMagicBytes(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte("invalid"))
	_, err := NewReader(io.NopCloser(&buf))
	assert.Error(t, err)
}

func TestReaderNext(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))

	data, err := (&types.Record{Key: "key1", Timestamp: time.Time{}, Document: []byte("doc1")}).Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)

	rec, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec.Key)
	assert.Equal(t, "doc1", string(rec.Document))
}

func TestReaderNextEOF(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))

	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)

	rec, err := r.Next()
	assert.NoError(t, err)
	assert.Nil(t, rec)
}
