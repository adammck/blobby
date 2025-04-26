package xor

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
	f, err := Create(keys)
	require.NoError(t, err)
	info, err := f.Marshal()
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, TypeName, info.Type)
	require.NotEmpty(t, info.Data)

	// All inserted keys should be found (no false negatives)
	for i, key := range keys {
		if i%1000 == 0 { // Test every 1000th key to keep test fast
			require.True(t, f.Contains(key), "Key should be in filter: %s", key)
		}
	}

	// Keys not in the set should mostly return false (with few false positives)
	falsePositives := 0
	testCount := 10000
	for i := 0; i < testCount; i++ {
		notInSetKey := fmt.Sprintf("other-key-%d", rand.Intn(1000000))
		if f.Contains(notInSetKey) {
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
	_, err := Create([]string{})
	require.Error(t, err)

	// Test with invalid filter type
	invalidTypeFilter := api.Filter{
		Type: "invalid",
		Data: []byte{1, 2, 3},
	}
	_, err = Unmarshal(invalidTypeFilter)
	require.Error(t, err)

	// Test with empty filter data
	emptyDataFilter := api.Filter{
		Type: TypeName,
		Data: nil,
	}
	_, err = Unmarshal(emptyDataFilter)
	require.Error(t, err)

	// Test with corrupted filter data
	corruptedDataFilter := api.Filter{
		Type: TypeName,
		Data: []byte{1, 2, 3}, // Not a valid serialized filter
	}
	_, err = Unmarshal(corruptedDataFilter)
	require.Error(t, err)
}

func TestXorFilterWithTombstones(t *testing.T) {
	// Create a set of normal keys and tombstone keys
	normalKeyCount := 1000
	tombstoneKeyCount := 500

	normalKeys := make([]string, normalKeyCount)
	for i := 0; i < normalKeyCount; i++ {
		normalKeys[i] = fmt.Sprintf("normal-key-%d", i)
	}

	tombstoneKeys := make([]string, tombstoneKeyCount)
	for i := 0; i < tombstoneKeyCount; i++ {
		tombstoneKeys[i] = fmt.Sprintf("deleted-key-%d", i)
	}

	// Combine all keys for the filter
	allKeys := make([]string, 0, normalKeyCount+tombstoneKeyCount)
	allKeys = append(allKeys, normalKeys...)
	allKeys = append(allKeys, tombstoneKeys...)

	// Create a filter with both normal and tombstone keys
	f, err := Create(allKeys)
	require.NoError(t, err)

	// Verify all keys are included in the filter - both normal and tombstone keys
	// Sample some keys to keep test runtime reasonable
	for i := 0; i < normalKeyCount; i += 100 {
		require.True(t, f.Contains(normalKeys[i]), "Filter should contain normal key: %s", normalKeys[i])
	}

	for i := 0; i < tombstoneKeyCount; i += 50 {
		require.True(t, f.Contains(tombstoneKeys[i]), "Filter should contain tombstone key: %s", tombstoneKeys[i])
	}

	// The filter should have similar false positive behavior for both types of keys
	// since it doesn't distinguish between them
	falsePositiveCount := 0
	testCount := 1000

	for i := 0; i < testCount; i++ {
		nonExistentKey := fmt.Sprintf("not-in-filter-key-%d", i+10000)
		if f.Contains(nonExistentKey) {
			falsePositiveCount++
		}
	}

	falsePositiveRate := float64(falsePositiveCount) / float64(testCount)
	t.Logf("False positive rate for random keys: %.4f (%d out of %d)",
		falsePositiveRate, falsePositiveCount, testCount)

	// XOR filters typically have a low false positive rate
	require.Less(t, falsePositiveRate, 0.01, "False positive rate should be reasonable")
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

				f, err := Create(keys)
				require.NoError(b, err)
				info, err := f.Marshal()
				require.NoError(b, err)

				bitsPerKey := float64(len(info.Data)*8) / float64(count)
				b.ReportMetric(bitsPerKey, "bits/key")
				b.SetBytes(int64(len(info.Data)))

				// Prevent compiler optimizations
				if info.Data == nil {
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

	f, err := Create(keys)
	require.NoError(b, err)

	// Benchmark lookup for keys in the set
	b.Run("KeysInSet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := keys[i%numKeys]
			if !f.Contains(key) {
				b.Fatalf("Expected key to be in filter")
			}
		}
	})

	// Benchmark lookup for keys not in the set
	b.Run("KeysNotInSet", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("other-key-%d", i)
			_ = f.Contains(key)
		}
	})
}
