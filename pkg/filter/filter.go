package filter

import (
	"fmt"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter/mod"
	"github.com/adammck/blobby/pkg/filter/xor"
)

type Filter interface {
	Contains(key string) bool
	Marshal() (api.Filter, error)
}

func Create(ft string, keys []string) (Filter, error) {
	switch ft {
	case xor.TypeName:
		return xor.Create(keys)
	case mod.TypeName:
		return mod.Create(keys)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", ft)
	}
}

func Unmarshal(f api.Filter) (Filter, error) {
	switch f.Type {
	case xor.TypeName:
		return xor.Unmarshal(f)
	case mod.TypeName:
		return mod.Unmarshal(f)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", f.Type)
	}
}
