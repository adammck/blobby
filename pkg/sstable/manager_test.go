package sstable

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/impl/blobstore/mock"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setupManager() (context.Context, *Manager, clockwork.Clock) {
	ctx := context.Background()
	c := clockwork.NewFakeClock()
	manager := NewManager(mock.New(), c, NewFactory())
	return ctx, manager, c
}

func TestManagerFlushEmpty(t *testing.T) {
	ctx, mgr, _ := setupManager()

	ch := make(chan *types.Record)
	close(ch)

	_, _, _, err := mgr.Flush(ctx, ch)
	require.ErrorIs(t, err, ErrNoRecords)
}

func TestManagerFlush(t *testing.T) {
	ctx, mgr, c := setupManager()

	ch := make(chan *types.Record)
	go func() {
		ch <- &types.Record{
			Key:       "k1",
			Timestamp: c.Now(),
			Document:  []byte("doc1"),
		}
		ch <- &types.Record{
			Key:       "k2",
			Timestamp: c.Now().Add(time.Second),
			Document:  []byte("doc2"),
		}
		close(ch)
	}()

	meta, idx, _, err := mgr.Flush(ctx, ch)
	require.NoError(t, err)
	require.Equal(t, 2, meta.Count)
	require.Equal(t, "k1", meta.MinKey)
	require.Equal(t, "k2", meta.MaxKey)

	require.Greater(t, len(idx), 0)

	r, err := mgr.GetFull(ctx, meta.Filename())
	require.NoError(t, err)
	defer r.Close()

	recs := toMap(t, r)
	require.Contains(t, recs, "k1")
	require.Contains(t, recs, "k2")
	require.Equal(t, []byte("doc1"), recs["k1"].Document)
	require.Equal(t, []byte("doc2"), recs["k2"].Document)

	require.NotContains(t, recs, "test3")
	require.Nil(t, recs["test3"])
}

func TestManagerGetNonExistentFile(t *testing.T) {
	ctx, mgr, _ := setupManager()
	r, err := mgr.GetFull(ctx, "nonexistent.sstable")
	require.Error(t, err)
	require.Nil(t, r)
}

func TestManagerPartialRead(t *testing.T) {
	ctx, mgr, c := setupManager()

	ch := make(chan *types.Record)
	go func() {
		for i := 0; i < 10; i++ {
			ch <- &types.Record{
				Key:       fmt.Sprintf("key%d", i),
				Timestamp: c.Now(),
				Document:  []byte(fmt.Sprintf("doc%d", i)),
			}
		}
		close(ch)
	}()

	meta, idx, _, err := mgr.Flush(ctx, ch)
	require.NoError(t, err)
	require.Greater(t, len(idx), 0)

	r, err := mgr.GetRange(ctx, meta.Filename(), idx[0].Offset, 0)
	require.NoError(t, err)
	defer r.Close()

	rec, err := r.Next()
	require.NoError(t, err)
	require.Equal(t, "key0", rec.Key)
}
