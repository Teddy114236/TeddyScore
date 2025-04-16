package cache

import (
	"time"
)

// CacheItem 缓存项结构
type CacheItem struct {
	Value      interface{}
	Expiration int64
}

// Expired 判断缓存项是否已过期
func (item CacheItem) Expired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}
