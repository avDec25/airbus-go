package cache

import (
	"github.com/avDec25/airbus-go/constants"
	"github.com/patrickmn/go-cache"
)

var MemCache *cache.Cache

func init() {
	MemCache = cache.New(constants.DefaultCacheExpiration, 0)
}
