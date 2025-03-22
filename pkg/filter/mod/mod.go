package mod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/adammck/blobby/pkg/api"
)

const FilterType = "mod"

// mod.Filter is a filter that only contains keys where the ascii code of the
// last character is even. This is just for testing, because an actual bloom
// filter is too weird. This is more predictable.
type Filter struct {
	keys map[string]struct{}
}

func Unmarshal(f api.Filter) (*Filter, error) {
	if f.Type != FilterType {
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

	keysMap := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		keysMap[k] = struct{}{}
	}

	return &Filter{keys: keysMap}, nil
}

func Create(keys []string) (*Filter, error) {
	if len(keys) == 0 {
		return nil, errors.New("empty key set")
	}

	keysMap := make(map[string]struct{})
	for _, key := range keys {
		if len(key) > 0 {
			lastChar := key[len(key)-1]
			if lastChar%2 == 0 {
				keysMap[key] = struct{}{}
			}
		}
	}

	return &Filter{keys: keysMap}, nil
}

func (f *Filter) Contains(key string) bool {
	if len(key) == 0 {
		return false
	}

	lastChar := key[len(key)-1]
	if lastChar%2 != 0 {
		return false
	}

	_, exists := f.keys[key]
	return exists
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
		Type: FilterType,
		Data: data,
	}, nil
}
