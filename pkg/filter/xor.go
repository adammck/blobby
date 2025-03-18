package filter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/FastFilter/xorfilter"
	"github.com/adammck/blobby/pkg/api"
)

const (
	FilterTypeXor      = "xor"
	FilterVersionXorV1 = "v1"
)

type serializedFilter struct {
	Seed               uint64
	SegmentLength      uint32
	SegmentLengthMask  uint32
	SegmentCount       uint32
	SegmentCountLength uint32
	Fingerprints       []uint8
}

func (sf *serializedFilter) marshal() ([]byte, error) {
	headerSize := 8 + 4*4 // uint64 + 4*uint32
	buf := make([]byte, headerSize+len(sf.Fingerprints))

	binary.LittleEndian.PutUint64(buf[0:], sf.Seed)
	binary.LittleEndian.PutUint32(buf[8:], sf.SegmentLength)
	binary.LittleEndian.PutUint32(buf[12:], sf.SegmentLengthMask)
	binary.LittleEndian.PutUint32(buf[16:], sf.SegmentCount)
	binary.LittleEndian.PutUint32(buf[20:], sf.SegmentCountLength)

	copy(buf[headerSize:], sf.Fingerprints)
	return buf, nil
}

func (sf *serializedFilter) unmarshal(data []byte) error {
	headerSize := 8 + 4*4 // uint64 + 4*uint32
	if len(data) < headerSize {
		return errors.New("data too short for header")
	}

	sf.Seed = binary.LittleEndian.Uint64(data[0:])
	sf.SegmentLength = binary.LittleEndian.Uint32(data[8:])
	sf.SegmentLengthMask = binary.LittleEndian.Uint32(data[12:])
	sf.SegmentCount = binary.LittleEndian.Uint32(data[16:])
	sf.SegmentCountLength = binary.LittleEndian.Uint32(data[20:])

	sf.Fingerprints = make([]uint8, len(data)-headerSize)
	copy(sf.Fingerprints, data[headerSize:])

	return nil
}

type Filter struct {
	xf xorfilter.BinaryFuse8
}

func NewFilter(f api.FilterInfo) (*Filter, error) {
	if f.Type != FilterTypeXor {
		return nil, fmt.Errorf("bad type: %s", f.Type)
	}
	if f.Version != FilterVersionXorV1 {
		return nil, fmt.Errorf("bad version: %s", f.Version)
	}
	if len(f.Data) == 0 {
		return nil, errors.New("empty data")
	}

	var sf serializedFilter
	if err := sf.unmarshal(f.Data); err != nil {
		return nil, err
	}

	return &Filter{
		xf: xorfilter.BinaryFuse8{
			Seed:               sf.Seed,
			SegmentLength:      sf.SegmentLength,
			SegmentLengthMask:  sf.SegmentLengthMask,
			SegmentCount:       sf.SegmentCount,
			SegmentCountLength: sf.SegmentCountLength,
			Fingerprints:       sf.Fingerprints,
		},
	}, nil
}

func (f *Filter) Contains(key string) bool {
	h := fnv.New64a()
	h.Write([]byte(key))
	return f.xf.Contains(h.Sum64())
}

// Create creates a new XOR filter from a list of keys
func Create(keys []string) (api.FilterInfo, error) {
	if len(keys) == 0 {
		return api.FilterInfo{}, errors.New("empty key set")
	}

	// Convert keys to []uint64 using FastFilter's hashing
	hashes := make([]uint64, len(keys))
	for i, key := range keys {
		h := fnv.New64a()
		h.Write([]byte(key))
		hashes[i] = h.Sum64()
	}

	// Create the filter
	filter, err := xorfilter.PopulateBinaryFuse8(hashes)
	if err != nil {
		return api.FilterInfo{}, fmt.Errorf("create: %w", err)
	}
	if filter == nil {
		return api.FilterInfo{}, errors.New("nil filter")
	}

	// manually serialize the filter
	sf := serializedFilter{
		Seed:               filter.Seed,
		SegmentLength:      uint32(filter.SegmentLength),
		SegmentLengthMask:  uint32(filter.SegmentLengthMask),
		SegmentCount:       uint32(filter.SegmentCount),
		SegmentCountLength: uint32(filter.SegmentCountLength),
		Fingerprints:       filter.Fingerprints,
	}

	data, err := sf.marshal()
	if err != nil {
		return api.FilterInfo{}, err
	}

	return api.FilterInfo{
		Type:    FilterTypeXor,
		Version: FilterVersionXorV1,
		Data:    data,
	}, nil
}
