package blobby

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

// debug test to understand what's happening
func TestDebugSimpleIterator(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	// single iterator with two records
	iterators := []api.Iterator{
		&mockIterator{records: []mockRecord{
			{key: "apple", value: []byte("a"), timestamp: baseTime},
			{key: "zebra", value: []byte("z"), timestamp: baseTime},
		}},
	}

	compound := newCompoundIterator(ctx, iterators, []string{"iter1"})
	defer compound.Close()

	fmt.Printf("heap length after init: %d\n", compound.heap.Len())
	if compound.heap.Len() > 0 {
		fmt.Printf("first state in heap: key=%s\n", (*compound.heap)[0].key)
	}

	var keys []string
	for i := 0; i < 10 && compound.Next(ctx); i++ { // limit to prevent infinite loop
		key := compound.Key()
		fmt.Printf("iteration %d: key=%s, heap_len=%d\n", i, key, compound.heap.Len())
		if compound.heap.Len() > 0 {
			fmt.Printf("  next state in heap: key=%s\n", (*compound.heap)[0].key)
		}
		keys = append(keys, key)
	}

	fmt.Printf("final keys: %v\n", keys)
	require.Equal(t, []string{"apple", "zebra"}, keys)
}
