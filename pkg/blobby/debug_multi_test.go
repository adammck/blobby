package blobby

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

// debug test for multi-iterator ordering issue
func TestDebugMultiIteratorOrdering(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	// same setup as failing test
	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "zebra", value: []byte("z"), timestamp: baseTime},
			{key: "apple", value: []byte("a"), timestamp: baseTime},
		}},
		&mockIterator{records: []mockRecord{
			{key: "banana", value: []byte("b"), timestamp: baseTime},
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1", "iter2"})
	defer compound.Close()

	fmt.Printf("heap length after init: %d\n", compound.heap.Len())
	for i := 0; i < compound.heap.Len(); i++ {
		state := (*compound.heap)[i]
		fmt.Printf("  heap[%d]: key=%s, source=%s\n", i, state.key, state.source)
	}

	var keys []string
	for i := 0; i < 10 && compound.Next(ctx); i++ { // limit to prevent infinite loop
		key := compound.Key()
		fmt.Printf("iteration %d: key=%s, heap_len=%d\n", i, key, compound.heap.Len())
		for j := 0; j < compound.heap.Len(); j++ {
			state := (*compound.heap)[j]
			fmt.Printf("  heap[%d]: key=%s, source=%s\n", j, state.key, state.source)
		}
		keys = append(keys, key)
	}

	fmt.Printf("final keys: %v\n", keys)
	require.Equal(t, []string{"apple", "banana", "zebra"}, keys)
}
