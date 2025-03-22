package filter

import (
	"fmt"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter/xor"
)

type Filter interface {
	Contains(key string) bool
	Marshal() (api.Filter, error)
}

func Create(keys []string) (Filter, error) {
	return xor.Create(keys)
}

func Load(f api.Filter) (Filter, error) {
	switch f.Type {
	case xor.FilterType:
		return xor.New(f)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", f.Type)
	}
}
