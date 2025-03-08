// This package contains only interfaces to be used by other packages. The
// implementations of these should be in pkg/impl/whatever. To avoid circular
// deps, this package should import nothing from pkg.
package api

import "context"

type Index []IndexEntry

type IndexEntry struct {
	Key    string
	Offset int64
}

type IndexStore interface {
	StoreIndex(ctx context.Context, filename string, entries Index) error
	GetIndex(ctx context.Context, filename string) (Index, error)
	DeleteIndex(ctx context.Context, filename string) error
}
