package filter

import (
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
