package blobby

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

var (
	initCount = flag.Int("initcount", 1000, "number of keys to create initially")
	numOps    = flag.Int("numops", 3000, "number of operations to perform")

	pGet     = flag.Int("pget", 70, "probability of get")
	pPut     = flag.Int("pput", 20, "probability of put")
	pFlush   = flag.Int("pflush", 8, "probability of flush")
	pCompact = flag.Int("pcompact", 2, "probability of compaction")

	pHot  = flag.Int("phot", 50, "probability of hot key")
	pWarm = flag.Int("pwarm", 30, "probability of warm key")
	pCold = flag.Int("pcold", 20, "probability of cold key")

	nHot  = flag.Int("nhot", 10, "number of hot keys")
	nWarm = flag.Int("nwarm", 90, "number of warm keys")
	nCold = flag.Int("ncold", 900, "number of cold keys")
)

func TestMain(m *testing.M) {
	flag.Parse()
	m.Run()
}

// see flags above for docs.
type chaosTestConfig struct {
	initCount int
	numOps    int
	pGet      int
	pPut      int
	pFlush    int
	pCompact  int
	pHot      int
	pWarm     int
	pCold     int
	nHot      int
	nWarm     int
	nCold     int
}

func configFromFlags() chaosTestConfig {
	return chaosTestConfig{
		initCount: *initCount,
		numOps:    *numOps,
		pGet:      *pGet,
		pPut:      *pPut,
		pFlush:    *pFlush,
		pCompact:  *pCompact,
		pHot:      *pHot,
		pWarm:     *pWarm,
		pCold:     *pCold,
		nHot:      *nHot,
		nWarm:     *nWarm,
		nCold:     *nCold,
	}
}

func TestChaos(t *testing.T) {
	ctx, _, b := setup(t, clockwork.NewRealClock())
	runChaosTest(t, ctx, b, configFromFlags())
}

func runChaosTest(t *testing.T, ctx context.Context, b *Blobby, cfg chaosTestConfig) {
	state := &testState{
		values: make(map[string][]byte),
	}

	t.Log("Creating initial dataset...")
	for i := range cfg.initCount {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Appendf(nil, "value-%04d-v1", i)
		err := putOp{key: key, value: val}.run(t, ctx, b, state)
		require.NoError(t, err)
	}

	t.Logf("Spamming %d random ops....", cfg.numOps)
	totalOps := cfg.pGet + cfg.pPut + cfg.pFlush + cfg.pCompact
	for i := range cfg.numOps {
		var op operation
		p := rand.Intn(totalOps)

		switch {
		case p < cfg.pGet:
			op = getOp{key: selectKey(cfg)}

		case p < cfg.pGet+cfg.pPut:
			key := selectKey(cfg)
			val := fmt.Appendf(nil, "value-%s-v%d", key, rand.Int())
			op = putOp{key: key, value: val}

		case p < cfg.pGet+cfg.pPut+cfg.pFlush:
			op = flushOp{}

		default:
			op = compactOp{}
		}

		// delay a bit to avoid collisions, because millisecond resolution.
		// TODO: remove this stupid hack, use automatic backoff and retry.
		time.Sleep(1 * time.Millisecond)

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

func selectKey(cfg chaosTestConfig) string {
	p := rand.Intn(cfg.pHot + cfg.pWarm + cfg.pCold)
	switch {
	case p < cfg.pHot:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.nHot))

	case p < cfg.pHot+cfg.pWarm:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.nWarm)+cfg.nHot)

	default:
		return fmt.Sprintf("key-%04d", rand.Intn(cfg.nCold)+cfg.nHot+cfg.nWarm)
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
