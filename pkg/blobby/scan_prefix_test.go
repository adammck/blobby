package blobby

import (
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/blobby/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestScanPrefix(t *testing.T) {
	ctx := context.Background()
	fake := testutil.NewFakeBlobby()

	// put test data with various prefixes
	testData := map[string]string{
		"users:alice":    "alice data",
		"users:bob":      "bob data",
		"users:charlie":  "charlie data",
		"items:sword":    "sword data",
		"items:shield":   "shield data",
		"config:timeout": "30s",
		"config:retries": "3",
	}

	for key, value := range testData {
		_, err := fake.Put(ctx, key, []byte(value))
		require.NoError(t, err)
	}

	// test prefix scan for users
	iter, stats, err := fake.ScanPrefix(ctx, "users:")
	require.NoError(t, err)
	require.NotNil(t, stats)
	defer iter.Close()

	userResults := make(map[string]string)
	for iter.Next(ctx) {
		userResults[iter.Key()] = string(iter.Value())
	}
	require.NoError(t, iter.Err())

	expected := map[string]string{
		"users:alice":   "alice data",
		"users:bob":     "bob data",
		"users:charlie": "charlie data",
	}
	require.Equal(t, expected, userResults)
	require.Equal(t, 3, stats.RecordsReturned)

	// test prefix scan for items
	iter, stats, err = fake.ScanPrefix(ctx, "items:")
	require.NoError(t, err)
	defer iter.Close()

	itemResults := make(map[string]string)
	for iter.Next(ctx) {
		itemResults[iter.Key()] = string(iter.Value())
	}
	require.NoError(t, iter.Err())

	expected = map[string]string{
		"items:sword":  "sword data",
		"items:shield": "shield data",
	}
	require.Equal(t, expected, itemResults)
	require.Equal(t, 2, stats.RecordsReturned)

	// test prefix scan for config
	iter, stats, err = fake.ScanPrefix(ctx, "config:")
	require.NoError(t, err)
	defer iter.Close()

	configResults := make(map[string]string)
	for iter.Next(ctx) {
		configResults[iter.Key()] = string(iter.Value())
	}
	require.NoError(t, iter.Err())

	expected = map[string]string{
		"config:timeout": "30s",
		"config:retries": "3",
	}
	require.Equal(t, expected, configResults)
	require.Equal(t, 2, stats.RecordsReturned)

	// test non-existent prefix
	iter, stats, err = fake.ScanPrefix(ctx, "nonexistent:")
	require.NoError(t, err)
	defer iter.Close()

	require.False(t, iter.Next(ctx))
	require.NoError(t, iter.Err())
	require.Equal(t, 0, stats.RecordsReturned)
}

func TestScanPrefixRealBlobby(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create test data across multiple sources
	testData := map[string]string{
		"users:alice":  "alice data",
		"users:bob":    "bob data",
		"items:sword":  "sword data",
		"config:debug": "true",
	}

	for key, value := range testData {
		c.Advance(10 * time.Millisecond)
		_, err := b.Put(ctx, key, []byte(value))
		require.NoError(t, err)
	}

	// flush some to sstable
	c.Advance(1 * time.Hour)
	_, err := b.Flush(ctx)
	require.NoError(t, err)

	// add more data to new memtable
	c.Advance(10 * time.Millisecond)
	_, err = b.Put(ctx, "users:charlie", []byte("charlie data"))
	require.NoError(t, err)

	c.Advance(10 * time.Millisecond)
	_, err = b.Put(ctx, "items:shield", []byte("shield data"))
	require.NoError(t, err)

	// test prefix scan across sources
	iter, stats, err := b.ScanPrefix(ctx, "users:")
	require.NoError(t, err)
	defer iter.Close()

	userResults := make(map[string]string)
	for iter.Next(ctx) {
		userResults[iter.Key()] = string(iter.Value())
	}
	require.NoError(t, iter.Err())

	expected := map[string]string{
		"users:alice":   "alice data",
		"users:bob":     "bob data",
		"users:charlie": "charlie data",
	}
	require.Equal(t, expected, userResults)
	require.Equal(t, 3, stats.RecordsReturned)
	require.Greater(t, stats.MemtablesRead, 0)
	require.Greater(t, stats.SstablesRead, 0)
}

func TestScanPrefixWithTombstones(t *testing.T) {
	c := clockwork.NewFakeClockAt(time.Now().Truncate(time.Second))
	ctx, _, b := setup(t, c)

	// create initial data
	c.Advance(10 * time.Millisecond)
	_, err := b.Put(ctx, "users:alice", []byte("alice data"))
	require.NoError(t, err)

	c.Advance(10 * time.Millisecond)
	_, err = b.Put(ctx, "users:bob", []byte("bob data"))
	require.NoError(t, err)

	// flush to sstable
	c.Advance(1 * time.Hour)
	_, err = b.Flush(ctx)
	require.NoError(t, err)

	// delete one user (creates tombstone)
	c.Advance(10 * time.Millisecond)
	_, err = b.Delete(ctx, "users:alice")
	require.NoError(t, err)

	// add new user
	c.Advance(10 * time.Millisecond)
	_, err = b.Put(ctx, "users:charlie", []byte("charlie data"))
	require.NoError(t, err)

	// scan should skip deleted user
	iter, stats, err := b.ScanPrefix(ctx, "users:")
	require.NoError(t, err)
	defer iter.Close()

	results := make(map[string]string)
	for iter.Next(ctx) {
		results[iter.Key()] = string(iter.Value())
	}
	require.NoError(t, iter.Err())

	expected := map[string]string{
		"users:bob":     "bob data",
		"users:charlie": "charlie data",
	}
	require.Equal(t, expected, results)
	require.Equal(t, 2, stats.RecordsReturned)
}
