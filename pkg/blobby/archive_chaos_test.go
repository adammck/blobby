package blobby

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/adammck/blobby/pkg/blobby/testutil"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

var (
	seed = flag.Int("seed", 0, "random seed; default is current time")

	initCount = flag.Int("initcount", 1000, "number of keys to create initially")
	numOps    = flag.Int("numops", 3000, "number of operations to perform")

	pGet     = flag.Int("pget", 200, "probability of get")
	pPut     = flag.Int("pput", 200, "probability of put")
	pDelete  = flag.Int("pdelete", 20, "probability of delete")
	pScan    = flag.Int("pscan", 50, "probability of range scan")
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
	pDelete   int
	pScan     int
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
		pDelete:   *pDelete,
		pScan:     *pScan,
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
	th := testutil.NewHarness(b)
	rnd := getRand(t, int64(cfg.seed))

	t.Log("Creating initial dataset...")
	for i := range cfg.initCount {
		key := generateKey(i)
		val := fmt.Appendf(nil, "value-%04d-v1", i)
		err := th.Put(key, val).Run(t, ctx)
		require.NoError(t, err)
	}

	t.Logf("Spamming %d random ops....", cfg.numOps)
	totalOps := cfg.pGet + cfg.pPut + cfg.pDelete + cfg.pScan + cfg.pFlush + cfg.pCompact
	for i := range cfg.numOps {
		var op testutil.Op
		p := rnd.Intn(totalOps)

		switch {
		case p < cfg.pGet:
			key, _ := selectKey(cfg, rnd)
			op = th.Get(key)

		case p < cfg.pGet+cfg.pPut:
			key, _ := selectKey(cfg, rnd)
			val := fmt.Appendf(nil, "value-%s-v%05d-r%d", key, i, rnd.Int())
			op = th.Put(key, val)

		case p < cfg.pGet+cfg.pPut+cfg.pDelete:
			key, _ := selectKey(cfg, rnd)
			op = th.Delete(key)

		case p < cfg.pGet+cfg.pPut+cfg.pDelete+cfg.pScan:
			start, end := selectRange(cfg, rnd)
			op = th.Scan(start, end)

		case p < cfg.pGet+cfg.pPut+cfg.pDelete+cfg.pScan+cfg.pFlush:
			op = th.Flush()

		default:
			op = th.Compact()
		}

		err := op.Run(t, ctx)
		require.NoError(t, err, "op=%#v", op)
	}

	th.Verify(ctx, t)

	th.LogStats(t)
}

// generateKey creates a key string from a number
func generateKey(num int) string {
	return fmt.Sprintf("key-%04d", num)
}

// selectKey selects a key based on the hot/warm/cold probabilities
// Returns both the key string and its numeric value to avoid re-parsing
func selectKey(cfg chaosTestConfig, rng *rand.Rand) (string, int) {
	p := rng.Intn(cfg.pHot + cfg.pWarm + cfg.pCold)
	var num int
	switch {
	case p < cfg.pHot:
		num = rng.Intn(cfg.nHot)

	case p < cfg.pHot+cfg.pWarm:
		num = rng.Intn(cfg.nWarm) + cfg.nHot

	default:
		num = rng.Intn(cfg.nCold) + cfg.nHot + cfg.nWarm
	}
	return generateKey(num), num
}

// selectRange generates a random range for range scans
func selectRange(cfg chaosTestConfig, rng *rand.Rand) (string, string) {
	// generate ranges of different sizes for variety
	rangeType := rng.Intn(100)

	switch {
	case rangeType < 30: // 30% - small range (1-5 keys)
		start, startNum := selectKey(cfg, rng)
		endNum := startNum + rng.Intn(5) + 1
		end := generateKey(endNum)
		return start, end

	case rangeType < 60: // 30% - medium range (5-20 keys)
		start, startNum := selectKey(cfg, rng)
		endNum := startNum + rng.Intn(20) + 5
		end := generateKey(endNum)
		return start, end

	case rangeType < 85: // 25% - large range (hot+warm keys)
		return generateKey(0), generateKey(cfg.nHot+cfg.nWarm)

	case rangeType < 95: // 10% - prefix scan simulation
		prefix := fmt.Sprintf("key-%01d", rng.Intn(10))
		return prefix + "000", prefix + "999"

	default: // 5% - full scan
		return "", ""
	}
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
