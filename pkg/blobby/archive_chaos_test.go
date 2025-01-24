package blobby

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

// Note: Use bin/chaostest to run this. It's pretty slow.
func TestChaos(t *testing.T) {
	ctx, _, b := setup(t, clockwork.NewRealClock())

	state := &testState{
		values: make(map[string][]byte),
	}

	t.Log("Creating initial dataset...")
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := []byte(fmt.Sprintf("value-%04d-v1", i))
		err := putOp{key: key, value: val}.run(t, ctx, b, state)
		require.NoError(t, err)
	}

	const numOps = 3000
	const (
		pGet     = 0.70
		pPut     = 0.20
		pFlush   = 0.08
		pCompact = 0.02
	)

	t.Logf("Spamming %d random ops....", numOps)
	for i := 0; i < numOps; i++ {
		var op operation
		p := rand.Float32()

		switch {
		case p < pGet:
			op = getOp{key: selectKey()}

		case p < pGet+pPut:
			key := selectKey()
			val := []byte(fmt.Sprintf("value-%s-v%d", key, rand.Int()))
			op = putOp{key: key, value: val}

		case p < pGet+pPut+pFlush:
			op = flushOp{}

		default:
			op = compactOp{}
		}

		t.Logf("Op %d: %s", i+1, op)
		err := op.run(t, ctx, b, state)
		require.NoError(t, err)
	}

	t.Log("Verifying final state...")
	var verified int
	for key, expected := range state.values {
		actual, stats, err := b.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expected, actual,
			"key %s: expected %q, got %q from %s",
			key, expected, actual, stats.Source)
		verified++
	}
	t.Logf("Verified %d keys", verified)
}

type operation interface {
	run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error
	String() string
}

type testState struct {
	values map[string][]byte
}

func selectKey() string {
	p := rand.Float32()
	switch {

	case p < 0.5:
		// hot: 10 keys, 50% of ops
		return fmt.Sprintf("key-%04d", rand.Intn(10))

	case p < 0.8:
		// warm: 90 keys, 30% of ops
		return fmt.Sprintf("key-%04d", rand.Intn(90)+10)

	default:
		// cold: 900 keys, 20% of ops
		return fmt.Sprintf("key-%04d", rand.Intn(900)+100)
	}
}

type putOp struct {
	key   string
	value []byte
}

func (o putOp) String() string {
	return fmt.Sprintf("put %s=%q", o.key, o.value)
}

func (o putOp) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
	dest, err := b.Put(ctx, o.key, o.value)
	if err != nil {
		return fmt.Errorf("put: %v", err)
	}
	state.values[o.key] = o.value
	t.Logf("Put %s=%q -> %s", o.key, o.value, dest)
	return nil
}

type getOp struct {
	key string
}

func (o getOp) String() string {
	return fmt.Sprintf("get %s", o.key)
}

func (o getOp) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
	expected, exists := state.values[o.key]
	actual, stats, err := b.Get(ctx, o.key)
	if err != nil {
		return fmt.Errorf("get: %v", err)
	}

	if !exists {
		if actual != nil {
			return fmt.Errorf("key %s: expected nil, got %q from %s",
				o.key, actual, stats.Source)
		}
		return nil
	}

	if string(actual) != string(expected) {
		return fmt.Errorf("key %s: expected %q, got %q from %s",
			o.key, expected, actual, stats.Source)
	}

	t.Logf("Get %s=%q <- %s (scanned %d records in %d blobs)",
		o.key, actual, stats.Source, stats.RecordsScanned, stats.BlobsFetched)
	return nil
}

type flushOp struct{}

func (o flushOp) String() string {
	return "flush"
}

func (o flushOp) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
	stats, err := b.Flush(ctx)
	if err != nil {
		// special case. it's fine if there's nothing to flush.
		if errors.Is(err, blobstore.NoRecords) {
			t.Logf("Flush: no records.")
			return nil
		}

		return fmt.Errorf("flush: %v", err)
	}
	t.Logf("Flush: %d records -> %s, now active: %s",
		stats.Meta.Count, stats.BlobURL, stats.ActiveMemtable)
	return nil
}

type compactOp struct{}

func (o compactOp) String() string {
	return "compact"
}

func (o compactOp) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
	stats, err := b.Compact(ctx, CompactionOptions{})
	if err != nil {
		return fmt.Errorf("compact: %v", err)
	}
	// TODO: print all of the stats, not just the first.
	if len(stats) > 0 {
		t.Logf("Compact: %d input files, %d output files",
			len(stats[0].Inputs), len(stats[0].Outputs))
	}
	return nil
}
