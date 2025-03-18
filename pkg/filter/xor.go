package filter

import (
	"errors"
	"fmt"

	"github.com/FastFilter/xorfilter"
	"github.com/adammck/blobby/pkg/api"
)

const (
	FilterTypeXor      = "xor"
	FilterVersionXorV1 = "v1"
)

// NewXorFilter creates a new XOR filter from a list of keys
func NewXorFilter(keys []string) (api.FilterInfo, error) {
	if len(keys) == 0 {
		return api.FilterInfo{}, errors.New("cannot create filter with empty key set")
	}

	// Convert keys to []uint64 using FastFilter's hashing
	hashes := make([]uint64, len(keys))
	for i, key := range keys {
		hashes[i] = xorfilter.StringHasher64(key)
	}

	// Create the filter
	filter := xorfilter.PopulateBinaryFuse8(hashes)
	if filter == nil {
		return api.FilterInfo{}, errors.New("failed to create xor filter")
	}

	// Serialize to []byte
	data, err := filter.MarshalBinary()
	if err != nil {
		return api.FilterInfo{}, fmt.Errorf("failed to marshal filter: %w", err)
	}

	return api.FilterInfo{
		Type:    FilterTypeXor,
		Version: FilterVersionXorV1,
		Data:    data,
	}, nil
}

// Contains checks if a key might be in the filter
func Contains(filter api.FilterInfo, key string) (bool, error) {
	// Check filter type and version
	if filter.Type != FilterTypeXor {
		return false, fmt.Errorf("unsupported filter type: %s", filter.Type)
	}

	if filter.Version != FilterVersionXorV1 {
		return false, fmt.Errorf("unsupported xor filter version: %s", filter.Version)
	}

	if len(filter.Data) == 0 {
		return false, errors.New("empty filter data")
	}

	// Deserialize
	var xorFilter xorfilter.BinaryFuse8
	if err := xorFilter.UnmarshalBinary(filter.Data); err != nil {
		return false, fmt.Errorf("failed to unmarshal filter: %w", err)
	}

	// Check if key might be in the set
	hash := xorfilter.StringHasher64(key)
	return xorFilter.Contains(hash), nil
}
