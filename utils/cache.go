package utils

import (
	"gohbase/utils/cache"
	"time"
)

// Cache 对外暴露的全局缓存实例
var Cache *cache.MemoryCache

// InitCache 初始化缓存
func InitCache(defaultExpiration, cleanupInterval time.Duration) {
	cache.InitCache(defaultExpiration, cleanupInterval)
	Cache = cache.Cache
}
