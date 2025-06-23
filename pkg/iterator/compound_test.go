package iterator

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

type mockIter struct {
	records []mockRecord
	pos     int
	closed  bool
	err     error
}

type mockRecord struct {
	key       string
	value     []byte
	timestamp time.Time
}

func (m *mockIter) Next(ctx context.Context) bool {
	if m.closed || m.err != nil || m.pos >= len(m.records) {
		return false
	}

	m.pos++
	return true
}

func (m *mockIter) Key() string {
	if m.pos <= 0 || m.pos > len(m.records) {
		return ""
	}

	return m.records[m.pos-1].key
}

func (m *mockIter) Value() []byte {
	if m.pos <= 0 || m.pos > len(m.records) {
		return nil
	}

	return m.records[m.pos-1].value
}

func (m *mockIter) Timestamp() time.Time {
	if m.pos <= 0 || m.pos > len(m.records) {
		return time.Time{}
	}

	return m.records[m.pos-1].timestamp
}

func (m *mockIter) Err() error {
	return m.err
}

func (m *mockIter) Close() error {
	m.closed = true
	return nil
}

func TestIteratorHeapOrdering(t *testing.T) {
	ts := time.Now().Truncate(time.Second)

	states := []*iterState{
		{
			key:       "same",
			timestamp: ts.Add(1 * time.Second),
		},
		{
			key:       "same",
			timestamp: ts,
		},
		{
			key:       "different",
			timestamp: ts,
		},
	}

	h := iterHeap(states)
	heap.Init(&h)

	s1 := heap.Pop(&h).(*iterState)
	require.Equal(t, "different", s1.key)

	s2 := heap.Pop(&h).(*iterState)
	require.Equal(t, "same", s2.key)
	require.Equal(t, ts.Add(1*time.Second), s2.timestamp)

	s3 := heap.Pop(&h).(*iterState)
	require.Equal(t, "same", s3.key)
	require.Equal(t, ts, s3.timestamp)
}

func TestCompoundWithEmptyIterators(t *testing.T) {
	ctx := context.Background()

	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: nil},
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: ts},
		}},
		&mockIter{records: nil},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, "key1", compound.Key())
	require.Equal(t, []byte("val1"), compound.Value())

	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}

func TestCompoundDuplicateHandling(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("newer"), timestamp: ts.Add(1 * time.Second)},
			{key: "key2", value: []byte("val2"), timestamp: ts},
		}},
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("older"), timestamp: ts},
			{key: "key3", value: []byte("val3"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	res := make(map[string][]byte)
	for compound.Next(ctx) {
		res[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	require.Equal(t, map[string][]byte{
		"key1": []byte("newer"),
		"key2": []byte("val2"),
		"key3": []byte("val3"),
	}, res)
}

func TestCompoundTombstoneHandling(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte{}, timestamp: ts.Add(1 * time.Second)},
			{key: "key2", value: []byte("live"), timestamp: ts},
		}},
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("old_value"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	res := make(map[string][]byte)
	for compound.Next(ctx) {
		res[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	require.Equal(t, map[string][]byte{
		"key2": []byte("live"),
	}, res)
}

func TestCompoundKeyOrdering(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "apple", value: []byte("a"), timestamp: ts},
			{key: "zebra", value: []byte("z"), timestamp: ts},
		}},
		&mockIter{records: []mockRecord{
			{key: "banana", value: []byte("b"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	var keys []string
	for compound.Next(ctx) {
		keys = append(keys, compound.Key())
	}
	require.NoError(t, compound.Err())

	require.Equal(t, []string{"apple", "banana", "zebra"}, keys)
}

func TestCompoundContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: ts},
			{key: "key2", value: []byte("val2"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, "key1", compound.Key())

	cancel()

	require.False(t, compound.Next(ctx))
	require.Error(t, compound.Err())
	require.Equal(t, context.Canceled, compound.Err())
}

func TestCompoundCloseResourceCleanup(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	mock1 := &mockIter{records: []mockRecord{
		{key: "key1", value: []byte("val1"), timestamp: ts},
	}}
	mock2 := &mockIter{records: []mockRecord{
		{key: "key2", value: []byte("val2"), timestamp: ts},
	}}

	iters := []api.Iterator{mock1, mock2}
	compound := New(ctx, iters)

	require.True(t, compound.Next(ctx))

	err := compound.Close()
	require.NoError(t, err)

	require.True(t, mock1.closed)
	require.True(t, mock2.closed)

	require.False(t, compound.Next(ctx))
	require.Empty(t, compound.Key())
	require.Nil(t, compound.Value())
}

func TestCompoundEmptyHeap(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "only", value: []byte("record"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, "only", compound.Key())

	require.False(t, compound.Next(ctx))
	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}

func TestCompoundTimestamp(t *testing.T) {
	ctx := context.Background()
	ts := time.Now().Truncate(time.Second)

	iters := []api.Iterator{
		&mockIter{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: ts},
		}},
	}

	compound := New(ctx, iters)
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, ts, compound.Timestamp())
}
