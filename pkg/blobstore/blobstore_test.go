package blobstore

import (
	"context"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/testdeps"
	"github.com/adammck/blobby/pkg/types"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *testdeps.Env, *Blobstore, clockwork.Clock) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMinio())
	clock := clockwork.NewFakeClock()
	bs := New(env.S3Bucket, clock)

	err := bs.Ping(ctx)
	require.NoError(t, err)

	return ctx, env, bs, clock
}

func TestFlushEmpty(t *testing.T) {
	ctx, _, bs, _ := setup(t)

	ch := make(chan *types.Record)
	close(ch)

	_, _, _, _, err := bs.Flush(ctx, ch)
	assert.ErrorIs(t, err, ErrNoRecords)
	//assert.Len(t, idx.Contents, 0)
}

func TestFlush(t *testing.T) {
	ctx, _, bs, clock := setup(t)

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

	_, n, meta, _, err := bs.Flush(ctx, ch)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "test1", meta.MinKey)
	assert.Equal(t, "test2", meta.MaxKey)

	// check that an index was written. the contents don't matter.
	//assert.Len(t, idx.Contents, 1)

	// TODO: Fix these. They were removed when Find was moved to Archive.

	// rec1, _, err := bs.Find(ctx, meta.Filename(), "test1")
	// require.NoError(t, err)
	// assert.NotNil(t, rec1)
	// assert.Equal(t, "test1", rec1.Key)
	// assert.Equal(t, []byte("doc1"), rec1.Document)

	// rec2, _, err := bs.Find(ctx, meta.Filename(), "test2")
	// require.NoError(t, err)
	// assert.NotNil(t, rec2)
	// assert.Equal(t, "test2", rec2.Key)
	// assert.Equal(t, []byte("doc2"), rec2.Document)

	// // unknown key
	// rec3, _, err := bs.Find(ctx, meta.Filename(), "test3")
	// require.NoError(t, err)
	// assert.Nil(t, rec3)
}

func TestGetNonExistentFile(t *testing.T) {
	ctx, _, bs, _ := setup(t)
	_, err := bs.GetFull(ctx, "nonexistent.sstable")
	assert.Error(t, err)
}
