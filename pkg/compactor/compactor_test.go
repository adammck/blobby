package compactor

import (
	"testing"
	"time"

	"github.com/adammck/archive/pkg/sstable"
	"github.com/stretchr/testify/require"
)

func TestGetCompactionsEmpty(t *testing.T) {
	c := &Compactor{}
	opts := CompactionOptions{
		MinFiles: 2,
	}
	compactions := c.GetCompactions([]*sstable.Meta{}, opts)
	require.Empty(t, compactions)
}

func TestGetCompactionsNotEnoughFiles(t *testing.T) {
	c := &Compactor{}
	opts := CompactionOptions{
		MinFiles: 2,
	}
	meta := &sstable.Meta{
		Created: time.Now(),
		Size:    100,
	}
	compactions := c.GetCompactions([]*sstable.Meta{meta}, opts)
	require.Empty(t, compactions)
}

func TestGetCompactionsOldestFirst(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now.Add(-2 * time.Hour), Size: 100},
		{Created: now.Add(-1 * time.Hour), Size: 100},
		{Created: now, Size: 100},
	}

	opts := CompactionOptions{
		Order:    OldestFirst,
		MinFiles: 2,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Equal(t, metas[0].Created, compactions[0].Inputs[0].Created)
	require.Equal(t, metas[1].Created, compactions[0].Inputs[1].Created)
	require.Equal(t, metas[2].Created, compactions[0].Inputs[2].Created)
}

func TestGetCompactionsNewestFirst(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now.Add(-2 * time.Hour), Size: 100},
		{Created: now.Add(-1 * time.Hour), Size: 100},
		{Created: now, Size: 100},
	}

	opts := CompactionOptions{
		Order:    NewestFirst,
		MinFiles: 2,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Equal(t, metas[2].Created, compactions[0].Inputs[0].Created)
	require.Equal(t, metas[1].Created, compactions[0].Inputs[1].Created)
	require.Equal(t, metas[0].Created, compactions[0].Inputs[2].Created)
}

func TestGetCompactionsSmallestFirst(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now, Size: 300},
		{Created: now, Size: 100},
		{Created: now, Size: 200},
	}

	opts := CompactionOptions{
		Order:    SmallestFirst,
		MinFiles: 2,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Equal(t, 100, compactions[0].Inputs[0].Size)
	require.Equal(t, 200, compactions[0].Inputs[1].Size)
	require.Equal(t, 300, compactions[0].Inputs[2].Size)
}

func TestGetCompactionsLargestFirst(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now, Size: 300},
		{Created: now, Size: 100},
		{Created: now, Size: 200},
	}

	opts := CompactionOptions{
		Order:    LargestFirst,
		MinFiles: 2,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Equal(t, 300, compactions[0].Inputs[0].Size)
	require.Equal(t, 200, compactions[0].Inputs[1].Size)
	require.Equal(t, 100, compactions[0].Inputs[2].Size)
}

func TestGetCompactionsMaxFiles(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now.Add(-3 * time.Hour), Size: 100},
		{Created: now.Add(-2 * time.Hour), Size: 100},
		{Created: now.Add(-1 * time.Hour), Size: 100},
		{Created: now, Size: 100},
	}

	opts := CompactionOptions{
		Order:    OldestFirst,
		MinFiles: 2,
		MaxFiles: 3,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Len(t, compactions[0].Inputs, 3)
}

func TestGetCompactionsMaxInputSize(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now.Add(-2 * time.Hour), Size: 100},
		{Created: now.Add(-1 * time.Hour), Size: 100},
		{Created: now, Size: 100},
	}

	opts := CompactionOptions{
		Order:        OldestFirst,
		MinFiles:     2,
		MaxInputSize: 250,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Len(t, compactions[0].Inputs, 2)
}

func TestGetCompactionsMinInputSize(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{Created: now.Add(-2 * time.Hour), Size: 100},
		{Created: now.Add(-1 * time.Hour), Size: 100},
		{Created: now, Size: 100},
	}

	opts := CompactionOptions{
		Order:        OldestFirst,
		MinFiles:     2,
		MinInputSize: 400,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Empty(t, compactions)
}

func TestGetCompactionsTimeFilter(t *testing.T) {
	c := &Compactor{}
	now := time.Now()

	metas := []*sstable.Meta{
		{
			Created: now.Add(-3 * time.Hour),
			Size:    100,
			MinTime: now.Add(-5 * time.Hour),
			MaxTime: now.Add(-4 * time.Hour), // All records too old
		},
		{
			Created: now.Add(-2 * time.Hour),
			Size:    100,
			MinTime: now.Add(-3 * time.Hour),
			MaxTime: now.Add(-1 * time.Hour), // Has records in range
		},
		{
			Created: now.Add(-1 * time.Hour),
			Size:    100,
			MinTime: now.Add(-2 * time.Hour),
			MaxTime: now.Add(-30 * time.Minute), // Has records in range
		},
		{
			Created: now,
			Size:    100,
			MinTime: now.Add(1 * time.Hour),
			MaxTime: now.Add(2 * time.Hour), // All records too new
		},
	}

	opts := CompactionOptions{
		Order:    OldestFirst,
		MinFiles: 2,
		MinTime:  now.Add(-3 * time.Hour),
		MaxTime:  now,
	}

	compactions := c.GetCompactions(metas, opts)
	require.Len(t, compactions, 1)
	require.Len(t, compactions[0].Inputs, 2)

	// Should include only the two files with records in our time range
	require.Equal(t, now.Add(-2*time.Hour), compactions[0].Inputs[0].Created)
	require.Equal(t, now.Add(-1*time.Hour), compactions[0].Inputs[1].Created)
}
