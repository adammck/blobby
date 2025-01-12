package integrationtest

import (
	"context"
	"testing"

	"github.com/adammck/archive/pkg/archive"
	"github.com/stretchr/testify/require"
)

func newTestArchive(t *testing.T, ctx context.Context) *archive.Archive {
	t.Helper()
	arc := archive.New(testEnv.mongoURI, testEnv.s3Bucket)
	arc.Init(ctx)
	arc.Ping(ctx)
	return arc
}

func TestBasicWriteRead(t *testing.T) {
	ctx := context.Background()
	a := newTestArchive(t, ctx)

	doc1 := []byte(`foo`)
	doc2 := []byte(`bar`)

	_, err := a.Put(ctx, "1", doc1)
	require.NoError(t, err)

	_, err = a.Put(ctx, "2", doc2)
	require.NoError(t, err)

	got, _, err := a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, got, doc1)

	got, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, got, doc2)

	_, _, _, err = a.Flush(ctx)
	require.NoError(t, err)

	got, _, err = a.Get(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, got, doc1)
	//require.Equal(t, src, "s3://test-bucket/1736651770.sstable") // TODO: inject a clock

	got, _, err = a.Get(ctx, "2")
	require.NoError(t, err)
	require.Equal(t, got, doc2)
}
