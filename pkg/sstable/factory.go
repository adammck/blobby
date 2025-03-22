package sstable

import (
	"github.com/jonboulle/clockwork"
)

type Factory interface {
	NewWriter() *Writer
}

type DefaultFactory struct {
	opts  []WriterOption
	clock clockwork.Clock
}

func NewFactory(clock clockwork.Clock, opts ...WriterOption) *DefaultFactory {
	return &DefaultFactory{
		opts:  opts,
		clock: clock,
	}
}

func (f *DefaultFactory) NewWriter() *Writer {
	return NewWriter(f.clock, f.opts...)
}
