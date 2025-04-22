package sstable

import (
	"github.com/jonboulle/clockwork"
)

type Factory interface {
	NewWriter(clockwork.Clock) *Writer
}

// TODO(adammck): Can we get rid of this? It's just []WriterOption now.
type DefaultFactory struct {
	opts []WriterOption
}

func NewFactory(opts ...WriterOption) *DefaultFactory {
	return &DefaultFactory{
		opts: opts,
	}
}

func (f *DefaultFactory) NewWriter(c clockwork.Clock) *Writer {
	return NewWriter(c, f.opts...)
}
