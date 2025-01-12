package archive

import (
	"context"
	"fmt"

	"github.com/adammck/archive/pkg/blobstore"
	"github.com/adammck/archive/pkg/memtable"
	"github.com/adammck/archive/pkg/metadata"
	"github.com/adammck/archive/pkg/sstable"
	"github.com/adammck/archive/pkg/types"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
)

type Archive struct {
	mt    *memtable.Memtable
	bs    *blobstore.Blobstore
	md    *metadata.Store
	clock clockwork.Clock
}

func New(mongoURL, bucket string, clock clockwork.Clock) *Archive {
	return &Archive{
		mt:    memtable.New(mongoURL, clock),
		bs:    blobstore.New(bucket, clock),
		md:    metadata.New(mongoURL),
		clock: clock,
	}
}

func (a *Archive) Ping(ctx context.Context) error {
	err := a.mt.Ping(ctx)
	if err != nil {
		return fmt.Errorf("memtable.Ping: %w", err)
	}

	err = a.bs.Ping(ctx)
	if err != nil {
		return fmt.Errorf("blobstore.Ping: %w", err)
	}

	return nil
}

func (a *Archive) Init(ctx context.Context) error {
	err := a.mt.Init(ctx)
	if err != nil {
		return fmt.Errorf("memtable.Init: %s", err)
	}

	err = a.md.Init(ctx)
	if err != nil {
		return fmt.Errorf("metadata.Init: %s", err)
	}

	return nil
}

func (a *Archive) Put(ctx context.Context, key string, value []byte) (string, error) {
	return a.mt.Put(ctx, key, value)
}

func (a *Archive) Get(ctx context.Context, key string) (value []byte, src string, err error) {
	rec, src, err := a.mt.Get(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("memtable.Get: %w", err)
	}
	if rec != nil {
		return rec.Document, src, nil
	}

	metas, err := a.md.GetContaining(ctx, key)
	if err != nil {
		return nil, "", fmt.Errorf("metadata.GetContaining: %w", err)
	}

	for _, meta := range metas {
		rec, src, err := a.bs.Get(ctx, meta.Filename(), key)
		if err != nil {
			return nil, "", fmt.Errorf("blobstore.Get: %w", err)
		}
		if rec != nil {
			return rec.Document, src, nil
		}
	}

	// key not found
	return nil, "", nil
}

func (a *Archive) Flush(ctx context.Context) (string, int, string, error) {

	// TODO: check whether old sstable is still flushing
	handle, mt, err := a.mt.Swap(ctx)
	if err != nil {
		return "", 0, "", fmt.Errorf("switchMemtable: %s", err)
	}

	ch := make(chan *types.Record)
	g, ctx2 := errgroup.WithContext(ctx)

	g.Go(func() error {
		var err error
		err = handle.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("memtable.Flush: %w", err)
		}
		return nil
	})

	var fn string
	var n int
	var meta *sstable.Meta

	g.Go(func() error {
		var err error
		fn, n, meta, err = a.bs.Flush(ctx2, ch)
		if err != nil {
			return fmt.Errorf("blobstore.Flush: %w", err)
		}
		return nil
	})

	err = g.Wait()
	if err != nil {
		return "", 0, "", err
	}

	err = a.md.Insert(ctx, meta)
	if err != nil {
		return "", 0, "", fmt.Errorf("metadata.Insert: %w", err)
	}

	err = handle.Truncate(ctx)
	if err != nil {
		return "", 0, "", fmt.Errorf("handle.Truncate: %w", err)
	}

	return fn, n, mt, nil
}
