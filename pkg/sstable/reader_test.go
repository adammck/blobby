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
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestReaderWithTombstone(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))

	// Write a tombstone record
	ts := time.Now().UTC()
	tombstoneRec := &types.Record{Key: "deleted-key", Timestamp: ts, Tombstone: true}
	data, err := tombstoneRec.Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	// Write a regular record
	regularRec := &types.Record{Key: "regular-key", Timestamp: ts, Document: []byte("regular-doc")}
	data, err = regularRec.Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)

	// Read the tombstone record
	rec1, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "deleted-key", rec1.Key)
	assert.True(t, rec1.Tombstone)
	assert.Nil(t, rec1.Document)

	// Read the regular record
	rec2, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "regular-key", rec2.Key)
	assert.False(t, rec2.Tombstone)
	assert.Equal(t, []byte("regular-doc"), rec2.Document)

	// Verify end of file
	rec3, err := r.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec3)
}

func TestReaderMultipleTombstones(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte(magicBytes))

	now := time.Now().UTC()

	// Write a regular record for key1
	data, err := (&types.Record{
		Key:       "key1",
		Timestamp: now.Add(-2 * time.Hour),
		Document:  []byte("original-value"),
	}).Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	// Write a tombstone for key1
	data, err = (&types.Record{
		Key:       "key1",
		Timestamp: now.Add(-1 * time.Hour),
		Tombstone: true,
	}).Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	// Write a new value for key1 (after the tombstone)
	data, err = (&types.Record{
		Key:       "key1",
		Timestamp: now,
		Document:  []byte("new-value"),
	}).Write(&buf)
	require.NoError(t, err)
	assert.NotZero(t, data)

	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)

	// Read all records and verify they have the correct properties
	rec1, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec1.Key)
	assert.False(t, rec1.Tombstone)
	assert.Equal(t, []byte("original-value"), rec1.Document)

	rec2, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec2.Key)
	assert.True(t, rec2.Tombstone)
	assert.Nil(t, rec2.Document)

	rec3, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec3.Key)
	assert.False(t, rec3.Tombstone)
	assert.Equal(t, []byte("new-value"), rec3.Document)

	// Verify end of file
	rec4, err := r.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec4)
}
