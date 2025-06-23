package blobby

import (
	"testing"

	"github.com/adammck/blobby/pkg/blobby/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

// TestScanConsistency tests the specific race condition found in chaos testing
func TestScanConsistency(t *testing.T) {
	ctx, _, b := setup(t, clockwork.NewRealClock())
	th := testutil.NewHarness(b)

	// setup initial key
	err := th.Put("key-0003", []byte("value-0003-v1")).Run(t, ctx)
	require.NoError(t, err)

	// force a flush to move data to sstable
	err = th.Flush().Run(t, ctx)
	require.NoError(t, err)

	// put a new value (goes to new memtable)
	err = th.Put("key-0003", []byte("value-key-0003-v00019-test")).Run(t, ctx)
	require.NoError(t, err)

	// range scan should see the new value
	err = th.Scan("key-0000", "key-0100").Run(t, ctx)
	require.NoError(t, err, "range scan should see the latest value after put")
}
