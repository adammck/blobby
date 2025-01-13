package archive

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *testutil.Env, *Archive, clockwork.Clock) {
	ctx := context.Background()
	env := testutil.SetupTest(ctx, t)

	clock := clockwork.NewFakeClock()
	arc := New(env.MongoURL, env.S3Bucket, clock)

	err := arc.Init(ctx)
	require.NoError(t, err)

	err = arc.Ping(ctx)
	require.NoError(t, err)

	return ctx, env, arc, clock
}

func TestBasicWriteRead(t *testing.T) {
	ctx, env, a, c := setup(t)

	// wrap archive in test helper to make this readable
	ta := &testArchive{
		ctx: ctx,
		t:   t,
		a:   a,
	}

	// prepare n docs full of junk
	docs := map[string][]byte{}
	for i := 1; i < 10; i++ {
		k := fmt.Sprintf("%03d", i)
		docs[k] = []byte(strings.Repeat(k, 3))
	}

	for k, v := range docs {
		ta.put(k, v)
	}

	// fetch an arbitrary key. they're all sitting in the default memtable
	// because we haven't flushed anything.
	val, gstats := ta.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, gstats, &GetStats{
		Source:         fmt.Sprintf("%s/archive/blue", env.MongoURL),
		BlobsFetched:   0,
		RecordsScanned: 0,
	})

	// and another one
	val, _ = ta.get("005")
	require.Equal(t, val, docs["005"])

	// flush memtable to the blobstore
	fstats, err := a.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, fstats, &FlushStats{
		FlushedMemtable: "",
		ActiveMemtable:  fmt.Sprintf("%s/archive/green", env.MongoURL),
		BlobURL:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, c.Now().Unix()),
		Meta: &sstable.Meta{
			MinKey:  "001",
			MaxKey:  "009",
			Count:   9,
			Size:    448, // idk lol
			Created: c.Now(),
		},
	})

	// fetch the same key, and see that it's now read from the blobstore.
	val, gstats = ta.get("001")
	require.Equal(t, val, docs["001"])
	require.Equal(t, gstats, &GetStats{
		Source:         fmt.Sprintf("s3://%s/%d.sstable", env.S3Bucket, c.Now().Unix()),
		BlobsFetched:   1,
		RecordsScanned: 1,
	})

	// fetch the other one to show how inefficient our linear scan is. yikes.
	val, gstats = ta.get("005")
	require.Equal(t, val, docs["005"])
	require.Equal(t, gstats.RecordsScanned, 5)
}

type testArchive struct {
	ctx context.Context
	t   *testing.T
	a   *Archive
}

func (ta *testArchive) put(key string, val []byte) string {
	dest, err := ta.a.Put(ta.ctx, key, val)
	require.NoError(ta.t, err)
	return dest
}

func (ta *testArchive) get(key string) ([]byte, *GetStats) {
	val, stats, err := ta.a.Get(ta.ctx, key)
	require.NoError(ta.t, err)
	return val, stats
}
