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
	pFlush   = flag.Int("pflush", 10, "probability of flush")
	pCompact = flag.Int("pcompact", 1, "probability of compaction")
	pBeginTx = flag.Int("pbegintx", 20, "probability of beginning a transaction")
	pTxPut   = flag.Int("ptxput", 50, "probability of put in transaction")
	pCommit  = flag.Int("pcommit", 70, "probability of committing vs aborting transaction")

	pHot  = flag.Int("phot", 50, "probability of hot key")
	pWarm = flag.Int("pwarm", 30, "probability of warm key")
	pCold = flag.Int("pcold", 20, "probability of cold key")

	nHot  = flag.Int("nhot", 10, "number of hot keys")
	nWarm = flag.Int("nwarm", 90, "number of warm keys")
	nCold = flag.Int("ncold", 900, "number of cold keys")
	
	maxActiveTx = flag.Int("maxtx", 5, "maximum number of active transactions")

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
	pBeginTx  int
	pTxPut    int
	pCommit   int
	pHot      int
	pWarm     int
	pCold     int
	nHot      int
	nWarm     int
	nCold     int
	maxTx     int
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
		pBeginTx:  *pBeginTx,
		pTxPut:    *pTxPut,
		pCommit:   *pCommit,
		pHot:      *pHot,
		pWarm:     *pWarm,
		pCold:     *pCold,
		nHot:      *nHot,
		nWarm:     *nWarm,
		nCold:     *nCold,
		maxTx:     *maxActiveTx,
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
	baseHarness := testutil.NewHarness(b)
	txh := testutil.NewTxHarness(baseHarness)
	rnd := getRand(t, int64(cfg.seed))

	t.Log("Creating initial dataset...")
	for i := range cfg.initCount {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Appendf(nil, "value-%04d-v1", i)
		err := baseHarness.Put(key, val).Run(t, ctx)
		require.NoError(t, err)
	}

	t.Logf("Spamming %d random ops....", cfg.numOps)
	totalOps := cfg.pGet + cfg.pPut + cfg.pFlush + cfg.pCompact + cfg.pBeginTx + cfg.pTxPut
	for i := range cfg.numOps {
		var op testutil.Op
		p := rnd.Intn(totalOps)

		switch {
		case p < cfg.pGet:
			op = baseHarness.Get(selectKey(cfg, rnd))

		case p < cfg.pGet+cfg.pPut:
			key := selectKey(cfg, rnd)
			val := fmt.Appendf(nil, "value-%s-v%05d-r%d", key, i, rnd.Int())
			op = baseHarness.Put(key, val)
			
		case p < cfg.pGet+cfg.pPut+cfg.pBeginTx:
			// Only begin new transactions if we're under the max limit
			if txh.NumActiveTx() < cfg.maxTx {
				op = txh.BeginTx()
				baseHarness.stats.TxBegin++
			} else {
				// Skip this operation
				continue
			}
			
		case p < cfg.pGet+cfg.pPut+cfg.pBeginTx+cfg.pTxPut:
			// Only do transaction puts if there are active transactions
			if activeTxs := txh.GetActiveTxIDs(); len(activeTxs) > 0 {
				// Choose a random active transaction
				txID := activeTxs[rnd.Intn(len(activeTxs))]
				key := selectKey(cfg, rnd)
				val := fmt.Appendf(nil, "tx-%s-value-%s-v%05d-r%d", txID, key, i, rnd.Int())
				op = txh.PutInTx(txID, key, val)
				baseHarness.stats.TxPuts++
			} else {
				// Skip this operation
				continue
			}
			
		case p < cfg.pGet+cfg.pPut+cfg.pBeginTx+cfg.pTxPut+cfg.pFlush:
			op = baseHarness.Flush()

		case p < cfg.pGet+cfg.pPut+cfg.pBeginTx+cfg.pTxPut+cfg.pFlush+cfg.pCompact:
			op = baseHarness.Compact()
			
		default:
			// End an existing transaction (commit or abort)
			if activeTxs := txh.GetActiveTxIDs(); len(activeTxs) > 0 {
				txID := activeTxs[rnd.Intn(len(activeTxs))]
				// Decide whether to commit or abort based on pCommit probability
				if rnd.Intn(100) < cfg.pCommit {
					op = txh.CommitTx(txID)
				} else {
					op = txh.AbortTx(txID)
				}
			} else {
				// Skip this operation
				continue
			}
		}

		err := op.Run(t, ctx)
		require.NoError(t, err, "op=%#v", op)
	}

	th.Verify(ctx, t)

	th.LogStats(t)
}

// selectKey selects a key based on the hot/warm/cold probabilities
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

// getRand returns a random number generator seeded with the given seed, unless
// the seed is 0, in which case it uses the current time.
func getRand(t *testing.T, seed int64) *rand.Rand {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	t.Logf("random seed: %d", seed)
	return rand.New(rand.NewSource(seed))
}
