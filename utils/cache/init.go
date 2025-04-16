package cache

import (
	"time"
)

// Cache 全局缓存实例
var Cache *MemoryCache

// InitCache 初始化缓存系统
func InitCache(defaultExpiration, cleanupInterval time.Duration) {
	Cache = NewMemoryCache(defaultExpiration, cleanupInterval)
}
