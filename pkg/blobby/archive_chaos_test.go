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

type ChaosTestConfig struct {
	// Number of keys to create initially
	InitCount int

	// Number of operations to perform
	NumOps int

	// Probability of each operation occuring
	PGet     int
	PPut     int
	PFlush   int
	PCompact int

	// Probability of selecting from each key distribution
	PHot  int
	PWarm int
	PCold int

	// Number of keys in each distribution
	HotKeys  int
	WarmKeys int
	ColdKeys int
}

// Note: Use bin/chaostest to run this. It's pretty slow.
func TestChaos(t *testing.T) {
	ctx, _, b := setup(t, clockwork.NewRealClock())

	config := ChaosTestConfig{
		InitCount: 1000,
		NumOps:    3000,
		PGet:      70,
		PPut:      20,
		PFlush:    8,
		PCompact:  2,

		// Key distribution config (matches original behavior)
		PHot:     50,  // 50% of ops
		PWarm:    30,  // 30% of ops
		PCold:    20,  // 20% of ops
		HotKeys:  10,  // first 10 keys
		WarmKeys: 90,  // next 90 keys
		ColdKeys: 900, // remaining 900 keys
	}

	runChaosTest(t, ctx, b, config)
}

func runChaosTest(t *testing.T, ctx context.Context, b *Blobby, cfg ChaosTestConfig) {
	state := &testState{
		values: make(map[string][]byte),
	}

	t.Log("Creating initial dataset...")
	for i := range cfg.InitCount {
		key := fmt.Sprintf("key-%04d", i)
		val := []byte(fmt.Sprintf("value-%04d-v1", i))
		err := putOp{key: key, value: val}.run(t, ctx, b, state)
		require.NoError(t, err)
	}

	t.Logf("Spamming %d random ops....", cfg.NumOps)
	totalOps := cfg.PGet + cfg.PPut + cfg.PFlush + cfg.PCompact
	for i := range cfg.NumOps {
		var op operation
		p := rand.Intn(totalOps)

		switch {
		case p < cfg.PGet:
			op = getOp{key: selectKey(cfg)}

		case p < cfg.PGet+cfg.PPut:
			key := selectKey(cfg)
			val := fmt.Appendf(nil, "value-%s-v%d", key, rand.Int())
			op = putOp{key: key, value: val}

		case p < cfg.PGet+cfg.PPut+cfg.PFlush:
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

func selectKey(cfg ChaosTestConfig) string {
	p := rand.Intn(cfg.PHot + cfg.PWarm + cfg.PCold)
	switch {
	case p < cfg.PHot:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.HotKeys))

	case p < cfg.PHot+cfg.PWarm:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.WarmKeys)+cfg.HotKeys)

	default:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.ColdKeys)+cfg.HotKeys+cfg.WarmKeys)
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
