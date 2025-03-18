package filter

import (
	"fmt"

	"github.com/adammck/blobby/pkg/api"
	"github.com/adammck/blobby/pkg/filter/xor"
)

type Filter interface {
	Contains(key string) bool
	Marshal() (api.FilterInfo, error)
}

func Create(keys []string) (Filter, error) {
	return xor.Create(keys)
}

func Load(info api.FilterInfo) (Filter, error) {
	switch info.Type {
	case xor.FilterType:
		return xor.New(info)
	default:
		return nil, fmt.Errorf("unknown filter type: %s", info.Type)
	}
}
