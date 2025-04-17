package blobby

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/blobstore"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

var (
	seed = flag.Int("seed", 0, "random seed; default is current time")

	initCount = flag.Int("initcount", 1000, "number of keys to create initially")
	numOps    = flag.Int("numops", 3000, "number of operations to perform")

	pGet     = flag.Int("pget", 200, "probability of get")
	pPut     = flag.Int("pput", 200, "probability of put")
	pFlush   = flag.Int("pflush", 10, "probability of flush")
	pCompact = flag.Int("pcompact", 1, "probability of compaction")

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
	seed      int
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
		seed:      *seed,
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
	if os.Getenv("BLOBBY_RUN_CHAOS") == "" {
		t.Skip("skipping chaos test; set BLOBBY_RUN_CHAOS=1 to run")
	}

	ctx, _, b := setup(t, clockwork.NewRealClock())
	runChaosTest(t, ctx, b, configFromFlags())
}

func runChaosTest(t *testing.T, ctx context.Context, b *Blobby, cfg chaosTestConfig) {
	state := &testState{
		values: make(map[string][]byte),
	}

	rnd := getRand(t, int64(cfg.seed))

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
		p := rnd.Intn(totalOps)

		switch {
		case p < cfg.pGet:
			op = getOp{key: selectKey(cfg, rnd)}

		case p < cfg.pGet+cfg.pPut:
			key := selectKey(cfg, rnd)
			val := fmt.Appendf(nil, "value-%s-v%05d-r%d", key, i, rnd.Int())
			op = putOp{key: key, value: val}

		case p < cfg.pGet+cfg.pPut+cfg.pFlush:
			op = flushOp{}

		default:
			op = compactOp{}
		}

		err := op.run(t, ctx, b, state)
		require.NoError(t, err, "op=%#v", op)
	}

	t.Log("Verifying final state...")
	var verified int
	for key, expected := range state.values {
		actual, stats, err := b.Get(ctx, key)
		require.NoError(t, err)
		require.Equal(t, expected, actual,
			"key %s: expected %q, got %q from %s",
			key, expected, actual, stats.Source)
		state.stats.incr(stats)
		verified++
	}
	t.Logf("Verified %d keys", verified)

	t.Log("Stats:")
	t.Logf("- blobs fetched: %d", state.stats.blobsFetched)
	t.Logf("- blobs skipped: %d", state.stats.blobsSkipped)
	t.Logf("- records scanned: %d", state.stats.totalRecordsScanned)
	t.Logf("- worst scan: %d recs", state.stats.maxRecordsScanned)
	t.Logf("- mean scan: %.1f recs", state.stats.meanRecordsScanned())
}

type operation interface {
	run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error
	String() string
}

type testState struct {
	values map[string][]byte
	stats  testStats
}

type testStats struct {
	numGets             uint64
	blobsFetched        uint64
	blobsSkipped        uint64
	totalRecordsScanned uint64
	maxRecordsScanned   uint64
}

func (s *testStats) incr(stats *api.GetStats) {
	s.numGets += 1
	s.blobsFetched += uint64(stats.BlobsFetched)
	s.blobsSkipped += uint64(stats.BlobsSkipped)
	s.totalRecordsScanned += uint64(stats.RecordsScanned)
	if uint64(stats.RecordsScanned) > s.maxRecordsScanned {
		s.maxRecordsScanned = uint64(stats.RecordsScanned)
	}
}

func (s *testStats) meanRecordsScanned() float64 {
	return float64(s.totalRecordsScanned) / float64(s.numGets)
}

func selectKey(cfg chaosTestConfig, rng *rand.Rand) string {
	p := rng.Intn(cfg.pHot + cfg.pWarm + cfg.pCold)
	switch {
	case p < cfg.pHot:
		return fmt.Sprintf("key-%04d", rng.Intn(cfg.nHot))

	case p < cfg.pHot+cfg.pWarm:
		return fmt.Sprintf("key-%04d", rng.Intn(cfg.nWarm)+cfg.nHot)

	default:
		return fmt.Sprintf("key-%04d", rng.Intn(cfg.nCold)+cfg.nHot+cfg.nWarm)
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

	state.stats.incr(stats)

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
		if errors.Is(err, blobstore.ErrNoRecords) {
			t.Logf("Flush: no records.")
			return nil
		}

		return fmt.Errorf("flush: %v", err)
	}
	t.Logf("Flush: %d records -> %s, now active: %s",
		stats.Meta.Count, stats.BlobName, stats.ActiveMemtable)
	return nil
}

type compactOp struct{}

func (o compactOp) String() string {
	return "compact"
}

func (o compactOp) run(t *testing.T, ctx context.Context, b *Blobby, state *testState) error {
	stats, err := b.Compact(ctx, api.CompactionOptions{})
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

// getRand returns a random number generator seeded with the given seed, unless
// the seed is 0, in which case it uses the current time.
func getRand(t *testing.T, seed int64) *rand.Rand {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	t.Logf("random seed: %d", seed)
	return rand.New(rand.NewSource(seed))
}
