package iterator

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

type mockIterator struct {
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

func (m *mockIterator) Next(ctx context.Context) bool {
	if m.closed || m.err != nil || m.pos >= len(m.records) {
		return false
	}
	m.pos++
	return true
}

func (m *mockIterator) Key() string {
	if m.pos <= 0 || m.pos > len(m.records) {
		return ""
	}
	return m.records[m.pos-1].key
}

func (m *mockIterator) Value() []byte {
	if m.pos <= 0 || m.pos > len(m.records) {
		return nil
	}
	return m.records[m.pos-1].value
}

func (m *mockIterator) CurrentTimestamp() time.Time {
	if m.pos <= 0 || m.pos > len(m.records) {
		return time.Time{}
	}
	return m.records[m.pos-1].timestamp
}

func (m *mockIterator) Err() error {
	return m.err
}

func (m *mockIterator) Close() error {
	m.closed = true
	return nil
}

func TestIteratorHeapOrdering(t *testing.T) {
	baseTime := time.Now().Truncate(time.Second)

	states := []*iteratorState{
		{
			key:       "same",
			timestamp: baseTime.Add(1 * time.Second),
		},
		{
			key:       "same",
			timestamp: baseTime,
		},
		{
			key:       "different",
			timestamp: baseTime,
		},
	}

	h := iteratorHeap(states)
	heap.Init(&h)

	state1 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "different", state1.key)

	state2 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "same", state2.key)
	require.Equal(t, baseTime.Add(1*time.Second), state2.timestamp)

	state3 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "same", state3.key)
	require.Equal(t, baseTime, state3.timestamp)
}

func TestCompoundWithEmptyIterators(t *testing.T) {
	ctx := context.Background()

	iterators := []api.Iterator{
		&mockIterator{records: nil},
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: time.Now()},
		}},
		&mockIterator{records: nil},
	}

	compound := NewCompound(ctx, iterators, []string{"empty1", "valid", "empty2"})
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, "key1", compound.Key())
	require.Equal(t, []byte("val1"), compound.Value())

	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}

func TestCompoundDuplicateHandling(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("newer"), timestamp: baseTime.Add(1 * time.Second)},
			{key: "key2", value: []byte("val2"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("older"), timestamp: baseTime},
			{key: "key3", value: []byte("val3"), timestamp: baseTime},
		}},
	}

	compound := NewCompound(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	results := make(map[string][]byte)
	for compound.Next(ctx) {
		results[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	expected := map[string][]byte{
		"key1": []byte("newer"),
		"key2": []byte("val2"),
		"key3": []byte("val3"),
	}
	require.Equal(t, expected, results)
}

func TestCompoundTombstoneHandling(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte{}, timestamp: baseTime.Add(1 * time.Second)},
			{key: "key2", value: []byte("live"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("old_value"), timestamp: baseTime},
		}},
	}

	compound := NewCompound(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	results := make(map[string][]byte)
	for compound.Next(ctx) {
		results[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	expected := map[string][]byte{
		"key2": []byte("live"),
	}
	require.Equal(t, expected, results)
}

func TestCompoundKeyOrdering(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "apple", value: []byte("a"), timestamp: baseTime},
			{key: "zebra", value: []byte("z"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "banana", value: []byte("b"), timestamp: baseTime},
		}},
	}

	compound := NewCompound(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	var keys []string
	for compound.Next(ctx) {
		keys = append(keys, compound.Key())
	}
	require.NoError(t, compound.Err())

	expected := []string{"apple", "banana", "zebra"}
	require.Equal(t, expected, keys)
}

func TestCompoundContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: time.Now()},
			{key: "key2", value: []byte("val2"), timestamp: time.Now()},
		}},
	}

	compound := NewCompound(ctx, iterators, []string{"iter1"})
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

	mock1 := &mockIterator{records: []mockRecord{
		{key: "key1", value: []byte("val1"), timestamp: time.Now()},
	}}
	mock2 := &mockIterator{records: []mockRecord{
		{key: "key2", value: []byte("val2"), timestamp: time.Now()},
	}}

	iterators := []api.Iterator{mock1, mock2}
	compound := NewCompound(ctx, iterators, []string{"iter1", "iter2"})

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

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "only", value: []byte("record"), timestamp: time.Now()},
		}},
	}

	compound := NewCompound(ctx, iterators, []string{"iter1"})
	defer compound.Close()

	require.True(t, compound.Next(ctx))
	require.Equal(t, "only", compound.Key())

	require.False(t, compound.Next(ctx))
	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}
