package sstable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRecord(t *testing.T) {
	w, c := newWriter()
	err := w.Add(&types.Record{Key: "key1", Timestamp: c.Now(), Document: []byte("doc1")})
	require.NoError(t, err)
	assert.Equal(t, 1, len(w.records))
	assert.Equal(t, "key1", w.records[0].Key)
}

func TestWriteRecords(t *testing.T) {
	w, c := newWriter()
	ts1 := c.Now()
	ts2 := ts1.Add(time.Hour)

	_ = w.Add(&types.Record{Key: "key1", Timestamp: ts1, Document: []byte("doc1")})
	_ = w.Add(&types.Record{Key: "key2", Timestamp: ts2, Document: []byte("doc2")})

	var buf bytes.Buffer
	meta, err := w.Write(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, meta.Count)
	assert.Equal(t, "key1", meta.MinKey)
	assert.Equal(t, "key2", meta.MaxKey)
	assert.Equal(t, ts1, meta.MinTime)
	assert.Equal(t, ts2, meta.MaxTime)
}

func TestWriteRecordsReverseTimes(t *testing.T) {
	w, c := newWriter()
	ts1 := c.Now()
	ts2 := ts1.Add(time.Hour)

	// Add records with timestamps in reverse order
	_ = w.Add(&types.Record{Key: "key1", Timestamp: ts2, Document: []byte("doc1")})
	_ = w.Add(&types.Record{Key: "key2", Timestamp: ts1, Document: []byte("doc2")})

	var buf bytes.Buffer
	meta, err := w.Write(&buf)
	require.NoError(t, err)
	assert.Equal(t, ts1, meta.MinTime)
	assert.Equal(t, ts2, meta.MaxTime)
}

func TestWriteTimestampsWithSingleRecord(t *testing.T) {
	w, c := newWriter()
	ts := c.Now()
	_ = w.Add(&types.Record{Key: "key1", Timestamp: ts, Document: []byte("doc1")})

	var buf bytes.Buffer
	meta, err := w.Write(&buf)
	require.NoError(t, err)
	assert.Equal(t, ts, meta.MinTime)
	assert.Equal(t, ts, meta.MaxTime)
}

func TestConcurrentAdd(t *testing.T) {
	w, c := newWriter()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := w.Add(&types.Record{Key: "key1", Timestamp: c.Now(), Document: []byte("doc1")})
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	assert.Equal(t, 100, len(w.records))
}

func TestWriteError(t *testing.T) {
	w, c := newWriter()
	w.Add(&types.Record{Key: "key1", Timestamp: c.Now(), Document: []byte("doc1")})

	fw := &writeFailer{}
	_, err := w.Write(fw)
	assert.Error(t, err)
}

func TestWriteOrder(t *testing.T) {
	w, c := newWriter()

	// truncate and convert to UTC to make assertions easier, since the round-
	// trip through BSON does this.
	ts1 := c.Now().UTC().Truncate(time.Millisecond)
	ts2 := ts1.Add(time.Hour)
	ts3 := ts2.Add(time.Hour)

	// add records out of order.
	w.Add(&types.Record{Key: "key2", Timestamp: ts1, Document: []byte("k2t1")})
	w.Add(&types.Record{Key: "key1", Timestamp: ts2, Document: []byte("k1t2")})
	w.Add(&types.Record{Key: "key2", Timestamp: ts3, Document: []byte("k2t3")})
	w.Add(&types.Record{Key: "key1", Timestamp: ts1, Document: []byte("k1t1")})

	var buf bytes.Buffer
	_, err := w.Write(&buf)
	require.NoError(t, err)

	r, err := NewReader(&buf)
	require.NoError(t, err)

	rec1, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec1.Key)
	assert.Equal(t, ts2, rec1.Timestamp)
	assert.Equal(t, []byte("k1t2"), rec1.Document)

	rec2, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key1", rec2.Key)
	assert.Equal(t, ts1, rec2.Timestamp)
	assert.Equal(t, []byte("k1t1"), rec2.Document)

	rec3, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key2", rec3.Key)
	assert.Equal(t, ts3, rec3.Timestamp)
	assert.Equal(t, []byte("k2t3"), rec3.Document)

	rec4, err := r.Next()
	require.NoError(t, err)
	assert.Equal(t, "key2", rec4.Key)
	assert.WithinDuration(t, ts1, rec4.Timestamp, 0)
	assert.Equal(t, []byte("k2t1"), rec4.Document)

	// EOF
	rec5, err := r.Next()
	require.Nil(t, err)
	assert.Nil(t, rec5)
}

type writeFailer struct{}

func (w *writeFailer) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("injected write failure")
}

func newWriter() (*Writer, clockwork.Clock) {
	c := clockwork.NewFakeClock()
	w := NewWriter(c)
	return w, c
}
