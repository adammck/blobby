package mod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/adammck/blobby/pkg/api"
)

const TypeName = "mod"

// mod.Filter is a filter that is accurate for keys where the last byte is odd
// (e.g. "1" (49), or "A" (65)), but always returns true when it's even (e.g.
// "0" (48), or "B" (66)), which may be a false positive depending on what has
// been written into the filter. This is just for testing, so we can easily
// predict which keys will be stored.
type Filter struct {
	keys map[string]struct{}
}

func Unmarshal(f api.Filter) (*Filter, error) {
	if f.Type != TypeName {
		return nil, fmt.Errorf("bad type: %s", f.Type)
	}
	if len(f.Data) == 0 {
		return nil, errors.New("empty data")
	}

	var keys []string
	err := json.Unmarshal(f.Data, &keys)
	if err != nil {
		return nil, err
	}

	km := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		km[k] = struct{}{}
	}

	return &Filter{keys: km}, nil
}

func Create(keys []string) (*Filter, error) {
	km := make(map[string]struct{})
	for _, key := range keys {
		km[key] = struct{}{}
	}

	return &Filter{keys: km}, nil
}

func (f *Filter) Contains(key string) bool {
	if len(key) == 0 {
		return true
	}

	chr := key[len(key)-1]
	if chr%2 == 0 {
		// it's even, so return true. maybe accurate, maybe false positive.
		return true
	}

	_, ok := f.keys[key]
	return ok
}

func (f *Filter) Marshal() (api.Filter, error) {
	keys := make([]string, 0, len(f.keys))
	for k := range f.keys {
		keys = append(keys, k)
	}

	data, err := json.Marshal(keys)
	if err != nil {
		return api.Filter{}, err
	}

	return api.Filter{
		Type: TypeName,
		Data: data,
	}, nil
}
