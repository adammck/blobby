package filter

import (
	"bytes"
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
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, sf.Seed); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, sf.SegmentLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, sf.SegmentLengthMask); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, sf.SegmentCount); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, sf.SegmentCountLength); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, sf.Fingerprints); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sf *serializedFilter) unmarshal(data []byte) error {
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, &sf.Seed); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &sf.SegmentLength); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &sf.SegmentLengthMask); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &sf.SegmentCount); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.LittleEndian, &sf.SegmentCountLength); err != nil {
		return err
	}
	sf.Fingerprints = make([]uint8, buf.Len())
	if err := binary.Read(buf, binary.LittleEndian, &sf.Fingerprints); err != nil {
		return err
	}
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
