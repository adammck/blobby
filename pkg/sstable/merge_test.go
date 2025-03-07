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

// claude wrote these. they look right, but i haven't reviewed them carefully.
// TODO: review them carefully!

func TestMergeReaderEmpty(t *testing.T) {
	m, err := NewMergeReader([]*Reader{})
	require.NoError(t, err)

	rec, err := m.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestMergeReaderSingleReader(t *testing.T) {
	now := time.Now().UTC()

	r := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now, Document: []byte("a1")},
		{Key: "b", Timestamp: now, Document: []byte("b1")},
	})

	m, err := NewMergeReader([]*Reader{r})
	require.NoError(t, err)

	assertNextRecord(t, m, "a", now, "a1")
	assertNextRecord(t, m, "b", now, "b1")

	rec, err := m.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestMergeReaderKeyOrdering(t *testing.T) {
	now := time.Now().UTC()

	r1 := readerWithRecords(t, []*types.Record{
		{Key: "b", Timestamp: now, Document: []byte("b1")},
		{Key: "d", Timestamp: now, Document: []byte("d1")},
	})

	r2 := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now, Document: []byte("a1")},
		{Key: "c", Timestamp: now, Document: []byte("c1")},
	})

	m, err := NewMergeReader([]*Reader{r1, r2})
	require.NoError(t, err)

	assertNextRecord(t, m, "a", now, "a1")
	assertNextRecord(t, m, "b", now, "b1")
	assertNextRecord(t, m, "c", now, "c1")
	assertNextRecord(t, m, "d", now, "d1")

	rec, err := m.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestMergeReaderVersionOrder(t *testing.T) {
	now := time.Now().UTC()
	hour := time.Hour

	r1 := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now.Add(-2 * hour), Document: []byte("oldest")},
		{Key: "b", Timestamp: now.Add(-1 * hour), Document: []byte("b-old")},
	})

	r2 := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now, Document: []byte("newest")},
		{Key: "b", Timestamp: now, Document: []byte("b-new")},
	})

	m, err := NewMergeReader([]*Reader{r1, r2})
	require.NoError(t, err)

	assertNextRecord(t, m, "a", now, "newest")
	assertNextRecord(t, m, "a", now.Add(-2*hour), "oldest")
	assertNextRecord(t, m, "b", now, "b-new")
	assertNextRecord(t, m, "b", now.Add(-1*hour), "b-old")

	rec, err := m.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func TestMergeReaderExactSameTimestamp(t *testing.T) {
	now := time.Now().UTC()

	r1 := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now, Document: []byte("a1")},
	})

	r2 := readerWithRecords(t, []*types.Record{
		{Key: "a", Timestamp: now, Document: []byte("a2")},
	})

	m, err := NewMergeReader([]*Reader{r1, r2})
	require.NoError(t, err)

	rec1, err := m.Next()
	require.NoError(t, err)
	assert.Equal(t, "a", rec1.Key)
	assert.Equal(t, now.Unix(), rec1.Timestamp.Unix())

	rec2, err := m.Next()
	require.NoError(t, err)
	assert.Equal(t, "a", rec2.Key)
	assert.Equal(t, now.Unix(), rec2.Timestamp.Unix())

	rec, err := m.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, rec)
}

func readerWithRecords(t *testing.T, records []*types.Record) *Reader {
	var buf bytes.Buffer
	buf.WriteString(magicBytes)

	for _, record := range records {
		_, err := record.Write(&buf)
		require.NoError(t, err)
	}

	r, err := NewReader(io.NopCloser(&buf))
	require.NoError(t, err)
	return r
}

func assertNextRecord(t *testing.T, m *MergeReader, expectedKey string, expectedTime time.Time, expectedDoc string) {
	rec, err := m.Next()
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, expectedKey, rec.Key)
	assert.Equal(t, expectedTime.Unix(), rec.Timestamp.Unix())
	assert.Equal(t, []byte(expectedDoc), rec.Document)
}
