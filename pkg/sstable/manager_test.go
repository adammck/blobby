package sstable

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/impl/blobstore/mock"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupManager() (context.Context, *Manager, clockwork.Clock) {
	ctx := context.Background()
	clock := clockwork.NewFakeClock()
	store := mock.New() // use mock store instead of real s3
	factory := NewFactory(clock)
	manager := NewManager(store, clock, factory)

	return ctx, manager, clock
}

func TestManagerFlushEmpty(t *testing.T) {
	ctx, manager, _ := setupManager()

	ch := make(chan *types.Record)
	close(ch)

	_, _, _, err := manager.Flush(ctx, ch)
	assert.ErrorIs(t, err, ErrNoRecords)
}

func TestManagerFlush(t *testing.T) {
	ctx, manager, clock := setupManager()

	ch := make(chan *types.Record)
	go func() {
		ch <- &types.Record{
			Key:       "test1",
			Timestamp: clock.Now(),
			Document:  []byte("doc1"),
		}
		ch <- &types.Record{
			Key:       "test2",
			Timestamp: clock.Now().Add(time.Second),
			Document:  []byte("doc2"),
		}
		close(ch)
	}()

	meta, idx, _, err := manager.Flush(ctx, ch)
	require.NoError(t, err)
	assert.Equal(t, 2, meta.Count)
	assert.Equal(t, "test1", meta.MinKey)
	assert.Equal(t, "test2", meta.MaxKey)

	assert.Greater(t, len(idx), 0)

	sst, err := manager.GetFull(ctx, meta.Filename())
	require.NoError(t, err)
	recs := sstableToMap(t, sst)
	assert.Contains(t, recs, "test1")
	assert.Contains(t, recs, "test2")
	assert.Equal(t, []byte("doc1"), recs["test1"].Document)
	assert.Equal(t, []byte("doc2"), recs["test2"].Document)

	assert.NotContains(t, recs, "test3")
	assert.Nil(t, recs["test3"])
}

func TestManagerGetNonExistentFile(t *testing.T) {
	ctx, manager, _ := setupManager()
	_, err := manager.GetFull(ctx, "nonexistent.sstable")
	assert.Error(t, err)
}

func TestManagerPartialRead(t *testing.T) {
	ctx, manager, clock := setupManager()

	ch := make(chan *types.Record)
	go func() {
		for i := 0; i < 10; i++ {
			ch <- &types.Record{
				Key:       fmt.Sprintf("key%d", i),
				Timestamp: clock.Now(),
				Document:  []byte(fmt.Sprintf("doc%d", i)),
			}
		}
		close(ch)
	}()

	meta, idx, _, err := manager.Flush(ctx, ch)
	require.NoError(t, err)
	require.Greater(t, len(idx), 0)

	partial, err := manager.GetRange(ctx, meta.Filename(), idx[0].Offset, 0)
	require.NoError(t, err)

	rec, err := partial.Next()
	require.NoError(t, err)
	assert.Equal(t, "key0", rec.Key)
}
