package xor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/FastFilter/xorfilter"
	"github.com/adammck/blobby/pkg/api"
)

const (
	FilterType      = "xor"
	FilterVersionV1 = "v1"
	headerSize      = 8 + 4*4 // uint64 + 4*uint32
)

type Filter struct {
	xf xorfilter.BinaryFuse8
}

func New(f api.FilterInfo) (*Filter, error) {
	if f.Type != FilterType {
		return nil, fmt.Errorf("bad type: %s", f.Type)
	}
	if f.Version != FilterVersionV1 {
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
	return f.xf.Contains(hashKey(key))
}

func Create(keys []string) (api.FilterInfo, error) {
	if len(keys) == 0 {
		return api.FilterInfo{}, errors.New("empty key set")
	}

	hashes := make([]uint64, len(keys))
	for i, key := range keys {
		hashes[i] = hashKey(key)
	}

	filter, err := xorfilter.PopulateBinaryFuse8(hashes)
	if err != nil {
		return api.FilterInfo{}, fmt.Errorf("create: %w", err)
	}
	if filter == nil {
		return api.FilterInfo{}, errors.New("nil filter")
	}

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
		Type:    FilterType,
		Version: FilterVersionV1,
		Data:    data,
	}, nil
}

func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

type serializedFilter struct {
	Seed               uint64
	SegmentLength      uint32
	SegmentLengthMask  uint32
	SegmentCount       uint32
	SegmentCountLength uint32
	Fingerprints       []uint8
}

func (sf *serializedFilter) marshal() ([]byte, error) {
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
