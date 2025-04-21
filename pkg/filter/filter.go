package filter

import (
	"fmt"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter/mod"
	"github.com/adammck/blobby/pkg/filter/xor"
)

func Create(ft string, keys []string) (api.FilterDecoded, error) {
	switch ft {
	case xor.TypeName:
		return xor.Create(keys)
	case mod.TypeName:
		return mod.Create(keys)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", ft)
	}
}

func Unmarshal(f *api.FilterEncoded) (api.FilterDecoded, error) {
	switch f.Type {
	case xor.TypeName:
		return xor.Unmarshal(f)
	case mod.TypeName:
		return mod.Unmarshal(f)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", f.Type)
	}
}
