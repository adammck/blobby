package integrationtest

import (
	"context"
	"fmt"
	"testing"

	"github.com/adammck/archive/pkg/archive"
	"github.com/adammck/archive/pkg/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *testutil.Env, *archive.Archive, clockwork.Clock) {
	ctx := context.Background()
	env := testutil.SetupTest(ctx, t)

	clock := clockwork.NewFakeClock()
	arc := archive.New(env.MongoURL, env.S3Bucket, clock)

	err := arc.Init(ctx)
	require.NoError(t, err)

	err = arc.Ping(ctx)
	require.NoError(t, err)

	return ctx, env, arc, clock
}

func TestBasicWriteRead(t *testing.T) {
	ctx, env, a, c := setup(t)

	doc1 := []byte(`foo`)
	doc2 := []byte(`bar`)

	_, err := a.Put(ctx, "1", doc1)
	require.NoError(t, err)

	_, err = a.Put(ctx, "2", doc2)
	require.NoError(t, err)

	val, src, err := a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, val, doc1)
	require.Equal(t, src, fmt.Sprintf("%s/archive/blue", env.MongoURL))

	val, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, val, doc2)

	_, _, _, err = a.Flush(ctx)
	require.NoError(t, err)

	val, src, err = a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, val, doc1)
	require.Equal(t, src, fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, c.Now().Unix()))

	val, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, val, doc2)
}
