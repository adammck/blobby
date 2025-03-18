// pkg/filter/xor_test.go
package filter

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/adammck/blobby/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestXorFilterBasics(t *testing.T) {
	// Create a set of random keys
	numKeys := 10000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", rand.Intn(1000000))
	}

	// Create a filter from those keys
	filter, err := NewXorFilter(keys)
	require.NoError(t, err)
	require.Equal(t, FilterTypeXor, filter.Type)
	require.Equal(t, FilterVersionXorV1, filter.Version)
	require.NotEmpty(t, filter.Data)

	// All inserted keys should be found (no false negatives)
	for i, key := range keys {
		if i%1000 == 0 { // Test every 1000th key to keep test fast
			contains, err := Contains(filter, key)
			require.NoError(t, err)
			require.True(t, contains, "Key should be in filter: %s", key)
		}
	}

	// Keys not in the set should mostly return false (with few false positives)
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		notInSetKey := fmt.Sprintf("other-key-%d", rand.Intn(1000000))
		contains, err := Contains(filter, notInSetKey)
		require.NoError(t, err)
		if contains {
			falsePositives++
		}
	}

	// Calculate and log the false positive rate
	falsePositiveRate := float64(falsePositives) / float64(testCount)
	t.Logf("False positive rate: %.4f (%d out of %d)", falsePositiveRate, falsePositives, testCount)

	// XOR filters typically have a false positive rate around 0.3-0.4%
	require.Less(t, falsePositiveRate, 0.01, "False positive rate should be reasonable")
}

func TestXorFilterErrors(t *testing.T) {
	// Test with empty keys
	_, err := NewXorFilter([]string{})
	require.Error(t, err)

	// Test with invalid filter type
	invalidTypeFilter := api.FilterInfo{
		Type:    "invalid",
		Version: FilterVersionXorV1,
		Data:    []byte{1, 2, 3},
	}
	_, err = Contains(invalidTypeFilter, "key")
	require.Error(t, err)

	// Test with invalid filter version
	invalidVersionFilter := api.FilterInfo{
		Type:    FilterTypeXor,
		Version: "invalid",
		Data:    []byte{1, 2, 3},
	}
	_, err = Contains(invalidVersionFilter, "key")
	require.Error(t, err)

	// Test with empty filter data
	emptyDataFilter := api.FilterInfo{
		Type:    FilterTypeXor,
		Version: FilterVersionXorV1,
		Data:    nil,
	}
	_, err = Contains(emptyDataFilter, "key")
	require.Error(t, err)

	// Test with corrupted filter data
	corruptedDataFilter := api.FilterInfo{
		Type:    FilterTypeXor,
		Version: FilterVersionXorV1,
		Data:    []byte{1, 2, 3}, // Not a valid serialized filter
	}
	_, err = Contains(corruptedDataFilter, "key")
	require.Error(t, err)
}

func BenchmarkXorFilterSize(b *testing.B) {
	keyCounts := []int{1000, 10000, 100000, 1000000}

	for _, count := range keyCounts {
		b.Run(fmt.Sprintf("Keys-%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				keys := make([]string, count)
				for j := 0; j < count; j++ {
					keys[j] = fmt.Sprintf("key-%d", j)
				}
				b.StartTimer()

				filter, err := NewXorFilter(keys)
				if err != nil {
					b.Fatalf("Failed to create filter: %v", err)
				}

				bitsPerKey := float64(len(filter.Data)*8) / float64(count)
				b.ReportMetric(bitsPerKey, "bits/key")
				b.SetBytes(int64(len(filter.Data)))

				// Prevent compiler optimizations
				if filter.Data == nil {
					b.Fatalf("Unexpected nil filter data")
				}
			}
		})
	}
}

func BenchmarkXorFilterContains(b *testing.B) {
	// Create a filter with 100K keys
	numKeys := 100000
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	filter, err := NewXorFilter(keys)
	if err != nil {
		b.Fatalf("Failed to create filter: %v", err)
	}

	// Benchmark lookup for keys in the set
	b.Run("KeysInSet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%numKeys]
			contains, err := Contains(filter, key)
			if err != nil || !contains {
				b.Fatalf("Expected key to be in filter: %v, err: %v", contains, err)
			}
		}
	})

	// Benchmark lookup for keys not in the set
	b.Run("KeysNotInSet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("other-key-%d", i)
			_, err := Contains(filter, key)
			if err != nil {
				b.Fatalf("Error checking filter: %v", err)
			}
		}
	})
}
