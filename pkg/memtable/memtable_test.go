package memtable

import (
	"context"
	"fmt"
	"testing"

	"github.com/adammck/archive/pkg/testdeps"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestSwap(t *testing.T) {
	ctx := context.Background()
	env := testdeps.New(ctx, t, testdeps.WithMongo())
	c := clockwork.NewFakeClock()
	mt := New(env.MongoURL(), c)

	err := mt.Init(ctx)
	require.NoError(t, err)

	// write to default table (blue)
	dest1, err := mt.Put(ctx, "k1", []byte("v1"))
	require.NoError(t, err)
	require.Equal(t, blueMemtableName, dest1)

	// first swap: blue -> green
	hNow, hPrev, err := mt.Swap(ctx)
	require.NoError(t, err)
	require.Equal(t, greenMemtableName, hPrev.Name())

	// write to now-active table (green)
	dest2, err := mt.Put(ctx, "k2", []byte("v2"))
	require.NoError(t, err)
	require.Equal(t, greenMemtableName, dest2)

	// verify both writes can be read back
	rec1, src1, err := mt.Get(ctx, "k1")
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), rec1.Document)
	require.Equal(t, dest1, src1)
	rec2, src2, err := mt.Get(ctx, "k2")
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), rec2.Document)
	require.Equal(t, dest2, src2)

	// try to swap back: green -> blue
	// fails because we haven't truncated blue
	_, _, err = mt.Swap(ctx)
	require.EqualError(t, err, fmt.Sprintf("want to activate %s, but is not empty", blueMemtableName))

	// so truncate it, to drop all the data
	// (note that we didn't flush, we're not testing that here.)
	err = hNow.Truncate(ctx)
	require.NoError(t, err)

	// try to swap back again: green -> blue
	// it works this time
	_, hNow, err = mt.Swap(ctx)
	require.NoError(t, err)
	require.Equal(t, blueMemtableName, hNow.Name())
}
