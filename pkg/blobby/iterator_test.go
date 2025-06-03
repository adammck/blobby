package blobby

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

// mockIterator implements api.Iterator and timestampProvider for testing
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

// TestIteratorHeapOrdering tests the heap implementation directly
func TestIteratorHeapOrdering(t *testing.T) {
	baseTime := time.Now().Truncate(time.Second)

	// create states with same key but different timestamps
	states := []*iteratorState{
		{
			key:       "same",
			timestamp: baseTime.Add(1 * time.Second), // newer
		},
		{
			key:       "same",
			timestamp: baseTime, // older
		},
		{
			key:       "different",
			timestamp: baseTime,
		},
	}

	h := iteratorHeap(states)
	heap.Init(&h)

	// should get "different" first (lexicographically smaller)
	state1 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "different", state1.key)

	// then "same" with newer timestamp
	state2 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "same", state2.key)
	require.Equal(t, baseTime.Add(1*time.Second), state2.timestamp)

	// finally "same" with older timestamp
	state3 := heap.Pop(&h).(*iteratorState)
	require.Equal(t, "same", state3.key)
	require.Equal(t, baseTime, state3.timestamp)
}

// TestCompoundIteratorWithEmptyIterators tests handling of empty iterators
func TestCompoundIteratorWithEmptyIterators(t *testing.T) {
	ctx := context.Background()

	// mix of empty and non-empty iterators
	iterators := []api.Iterator{
		&mockIterator{records: nil}, // empty
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: time.Now()},
		}},
		&mockIterator{records: nil}, // empty
	}

	compound := newCompoundIterator(ctx, iterators, []string{"empty1", "valid", "empty2"})
	defer compound.Close()

	// should get the one valid record
	require.True(t, compound.Next(ctx))
	require.Equal(t, "key1", compound.Key())
	require.Equal(t, []byte("val1"), compound.Value())

	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}

// TestCompoundIteratorDuplicateHandling tests that only newest version is returned
func TestCompoundIteratorDuplicateHandling(t *testing.T) {
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

	compound := newCompoundIterator(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	results := make(map[string][]byte)
	for compound.Next(ctx) {
		results[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	// should get newer version of key1, and both key2 and key3
	expected := map[string][]byte{
		"key1": []byte("newer"),
		"key2": []byte("val2"),
		"key3": []byte("val3"),
	}
	require.Equal(t, expected, results)
}

// TestCompoundIteratorTombstoneHandling tests tombstone (empty value) handling
func TestCompoundIteratorTombstoneHandling(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte{}, timestamp: baseTime.Add(1 * time.Second)}, // tombstone
			{key: "key2", value: []byte("live"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("old_value"), timestamp: baseTime}, // older version
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	results := make(map[string][]byte)
	for compound.Next(ctx) {
		results[compound.Key()] = compound.Value()
	}
	require.NoError(t, compound.Err())

	// should only get key2, key1 is tombstoned
	expected := map[string][]byte{
		"key2": []byte("live"),
	}
	require.Equal(t, expected, results)
}

// TestCompoundIteratorKeyOrdering tests that keys are returned in lexicographic order
func TestCompoundIteratorKeyOrdering(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	// iterators with keys in sorted order within each iterator
	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "apple", value: []byte("a"), timestamp: baseTime},
			{key: "zebra", value: []byte("z"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "banana", value: []byte("b"), timestamp: baseTime},
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	var keys []string
	for compound.Next(ctx) {
		keys = append(keys, compound.Key())
	}
	require.NoError(t, compound.Err())

	// should be in lexicographic order
	expected := []string{"apple", "banana", "zebra"}
	require.Equal(t, expected, keys)
}

// TestCompoundIteratorContextCancellation tests context cancellation handling
func TestCompoundIteratorContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "key1", value: []byte("val1"), timestamp: time.Now()},
			{key: "key2", value: []byte("val2"), timestamp: time.Now()},
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1"})
	defer compound.Close()

	// get first record
	require.True(t, compound.Next(ctx))
	require.Equal(t, "key1", compound.Key())

	// cancel context
	cancel()

	// next should fail with context cancellation
	require.False(t, compound.Next(ctx))
	require.Error(t, compound.Err())
	require.Equal(t, context.Canceled, compound.Err())
}

// TestCompoundIteratorCloseResourceCleanup tests that Close() cleans up all iterators
func TestCompoundIteratorCloseResourceCleanup(t *testing.T) {
	ctx := context.Background()

	mock1 := &mockIterator{records: []mockRecord{
		{key: "key1", value: []byte("val1"), timestamp: time.Now()},
	}}
	mock2 := &mockIterator{records: []mockRecord{
		{key: "key2", value: []byte("val2"), timestamp: time.Now()},
	}}

	iterators := []api.Iterator{mock1, mock2}
	compound := newCompoundIterator(ctx, iterators, []string{"iter1", "iter2"})

	// consume one record
	require.True(t, compound.Next(ctx))

	// close should clean up all underlying iterators
	err := compound.Close()
	require.NoError(t, err)

	require.True(t, mock1.closed)
	require.True(t, mock2.closed)

	// subsequent operations should be safe
	require.False(t, compound.Next(ctx))
	require.Empty(t, compound.Key())
	require.Nil(t, compound.Value())
}

// TestCompoundIteratorEmptyHeap tests behavior when heap becomes empty
func TestCompoundIteratorEmptyHeap(t *testing.T) {
	ctx := context.Background()

	// single record iterator
	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "only", value: []byte("record"), timestamp: time.Now()},
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1"})
	defer compound.Close()

	// get the only record
	require.True(t, compound.Next(ctx))
	require.Equal(t, "only", compound.Key())

	// subsequent calls should return false
	require.False(t, compound.Next(ctx))
	require.False(t, compound.Next(ctx))
	require.NoError(t, compound.Err())
}
