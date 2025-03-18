package sstable

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
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
	meta, _, _, err := w.Write(&buf)
	require.NoError(t, err)
	assert.Equal(t, 2, meta.Count)
	assert.Equal(t, "key1", meta.MinKey)
	assert.Equal(t, "key2", meta.MaxKey)
	assert.Equal(t, ts1, meta.MinTime)
	assert.Equal(t, ts2, meta.MaxTime)
}

func TestWriteWithIndex(t *testing.T) {
	w, c := newWriter(WithIndexEveryNRecords(3))
	ts := c.Now()

	recs := []*types.Record{
		{Key: "key1", Timestamp: ts, Document: []byte("doc1")},
		{Key: "key2", Timestamp: ts, Document: []byte("doc2")},
		{Key: "key3", Timestamp: ts, Document: []byte("doc3")},
		{Key: "key4", Timestamp: ts, Document: []byte("doc4")},
		{Key: "key5", Timestamp: ts, Document: []byte("doc5")},
		{Key: "key6", Timestamp: ts, Document: []byte("doc6")},
		{Key: "key7", Timestamp: ts, Document: []byte("doc7")},
	}

	for _, r := range recs {
		err := w.Add(r)
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	_, idx, _, err := w.Write(&buf)
	require.NoError(t, err)
	require.Len(t, idx, 3)
	require.Equal(t, "key1", idx[0].Key)
	require.Equal(t, "key4", idx[1].Key)
	require.Equal(t, "key7", idx[2].Key)
}

func TestWriteWithIndexByteFreq(t *testing.T) {
	recNum := 20
	recSize := 77 // will become wrong if encoding changes

	w, c := newWriter(WithIndexEveryNBytes(256))
	ts := c.Now()

	for i := range recNum {
		err := w.Add(&types.Record{
			Key:       fmt.Sprintf("k%d", i+101),
			Timestamp: ts,
			Document:  []byte("abcdefghijklmnopqrstuvwxyz0123456789"), // 36 bytes
		})
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	meta, idx, _, err := w.Write(&buf)
	require.NoError(t, err)

	// if these fail, the test is broken, maybe not the implementation.
	require.Equal(t, recNum, meta.Count, "unexpected record count")
	require.Equal(t, int64(len(magicBytes)+(recNum*recSize)), meta.Size, "unexpected total size; check recSize")

	// 256 bytes per segment, 77 bytes per record -> 4 records per segment. the
	// index entry appears before the *next* record, so the size of each segment
	// will actually be slightly more than 256.
	require.Equal(t, []api.IndexEntry{
		{Key: "k101", Offset: int64(len(magicBytes))},
		{Key: "k105", Offset: int64(len(magicBytes) + (recSize * 4))},
		{Key: "k109", Offset: int64(len(magicBytes) + (recSize * 8))},
		{Key: "k113", Offset: int64(len(magicBytes) + (recSize * 12))},
		{Key: "k117", Offset: int64(len(magicBytes) + (recSize * 16))},
	}, idx)
}

func TestWriteRecordsReverseTimes(t *testing.T) {
	w, c := newWriter()
	ts1 := c.Now()
	ts2 := ts1.Add(time.Hour)

	// Add records with timestamps in reverse order
	_ = w.Add(&types.Record{Key: "key1", Timestamp: ts2, Document: []byte("doc1")})
	_ = w.Add(&types.Record{Key: "key2", Timestamp: ts1, Document: []byte("doc2")})

	var buf bytes.Buffer
	meta, _, _, err := w.Write(&buf)
	require.NoError(t, err)
	assert.Equal(t, ts1, meta.MinTime)
	assert.Equal(t, ts2, meta.MaxTime)
}

func TestWriteTimestampsWithSingleRecord(t *testing.T) {
	w, c := newWriter()
	ts := c.Now()
	_ = w.Add(&types.Record{Key: "key1", Timestamp: ts, Document: []byte("doc1")})

	var buf bytes.Buffer
	meta, _, _, err := w.Write(&buf)
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
	_, _, _, err := w.Write(fw)
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
	_, _, _, err := w.Write(&buf)
	require.NoError(t, err)

	r, err := NewReader(io.NopCloser(&buf))
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

func newWriter(opts ...WriterOption) (*Writer, clockwork.Clock) {
	c := clockwork.NewFakeClock()
	w := NewWriter(c, opts...)
	return w, c
}
