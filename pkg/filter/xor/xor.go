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

	xf, err := unmarshal(f.Data)
	if err != nil {
		return nil, err
	}

	return &Filter{xf: xf}, nil
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
		return api.FilterInfo{}, fmt.Errorf("PopulateBinaryFuse8: %w", err)
	}

	data, err := marshal(filter)
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

func marshal(filter *xorfilter.BinaryFuse8) ([]byte, error) {
	buf := make([]byte, headerSize+len(filter.Fingerprints))

	// header
	binary.LittleEndian.PutUint64(buf[0:], filter.Seed)
	binary.LittleEndian.PutUint32(buf[8:], filter.SegmentLength)
	binary.LittleEndian.PutUint32(buf[12:], filter.SegmentLengthMask)
	binary.LittleEndian.PutUint32(buf[16:], filter.SegmentCount)
	binary.LittleEndian.PutUint32(buf[20:], filter.SegmentCountLength)

	// body
	copy(buf[headerSize:], filter.Fingerprints)

	return buf, nil
}

func unmarshal(data []byte) (xorfilter.BinaryFuse8, error) {
	var xf xorfilter.BinaryFuse8

	if len(data) < headerSize {
		return xf, errors.New("data too short for header")
	}

	// header
	xf.Seed = binary.LittleEndian.Uint64(data[0:])
	xf.SegmentLength = binary.LittleEndian.Uint32(data[8:])
	xf.SegmentLengthMask = binary.LittleEndian.Uint32(data[12:])
	xf.SegmentCount = binary.LittleEndian.Uint32(data[16:])
	xf.SegmentCountLength = binary.LittleEndian.Uint32(data[20:])

	// body
	xf.Fingerprints = make([]uint8, len(data)-headerSize)
	copy(xf.Fingerprints, data[headerSize:])

	return xf, nil
}
