package integrationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adammck/archive/pkg/archive"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func newTestArchive(t *testing.T, ctx context.Context) (*archive.Archive, clockwork.Clock) {
	t.Helper()
	clock := clockwork.NewFakeClockAt(time.Unix(455968859, 0))
	arc := archive.New(testEnv.mongoURI, testEnv.s3Bucket, clock)
	arc.Init(ctx)
	arc.Ping(ctx)
	return arc, clock
}

func TestBasicWriteRead(t *testing.T) {
	ctx := context.Background()
	a, c := newTestArchive(t, ctx)

	doc1 := []byte(`foo`)
	doc2 := []byte(`bar`)

	_, err := a.Put(ctx, "1", doc1)
	require.NoError(t, err)

	_, err = a.Put(ctx, "2", doc2)
	require.NoError(t, err)

	val, src, err := a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, val, doc1)
	require.Equal(t, src, fmt.Sprintf("%s/archive/blue", testEnv.mongoURI))

	val, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, val, doc2)

	_, _, _, err = a.Flush(ctx)
	require.NoError(t, err)

	val, src, err = a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, val, doc1)
	require.Equal(t, src, fmt.Sprintf("s3://%s/%d.sstable", testEnv.s3Bucket, c.Now().Unix()))

	val, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, val, doc2)
}
