package mod

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/adammck/blobby/pkg/api"
)

const TypeName = "mod"

// mod.Filter is a filter that only contains keys where the ascii code of the
// last character is even. This is just for testing, because an actual bloom
// filter is too weird. This is more predictable.
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
	if len(keys) == 0 {
		return nil, errors.New("empty key set")
	}

	km := make(map[string]struct{})
	for _, key := range keys {
		if len(key) > 0 {
			chr := key[len(key)-1]
			if chr%2 == 0 {
				km[key] = struct{}{}
			}
		}
	}

	return &Filter{keys: km}, nil
}

func (f *Filter) Contains(key string) bool {
	if len(key) == 0 {
		return false
	}

	chr := key[len(key)-1]
	if chr%2 != 0 {
		return false
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
