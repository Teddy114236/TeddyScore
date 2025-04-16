package cache

import (
	"strings"
	"sync"
	"time"
)

// MemoryCache 内存缓存实现
type MemoryCache struct {
	items             map[string]CacheItem
	mu                sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	stopCleanup       chan bool
	hitCount          int64        // 缓存命中计数
	missCount         int64        // 缓存未命中计数
	hitCountMu        sync.RWMutex // 命中计数锁，避免与主缓存锁冲突
}

// NewMemoryCache 创建新的内存缓存
func NewMemoryCache(defaultExpiration, cleanupInterval time.Duration) *MemoryCache {
	cache := &MemoryCache{
		items:             make(map[string]CacheItem),
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
		stopCleanup:       make(chan bool),
		hitCount:          0,
		missCount:         0,
	}

	// 如果清理间隔大于0，启动后台清理协程
	if cleanupInterval > 0 {
		go cache.startCleanupTimer()
	}

	return cache
}

// Set 设置缓存项，使用默认过期时间
func (c *MemoryCache) Set(key string, value interface{}) {
	c.SetWithExpiration(key, value, c.defaultExpiration)
}

// SetWithExpiration 设置缓存项，指定过期时间
func (c *MemoryCache) SetWithExpiration(key string, value interface{}, duration time.Duration) {
	var expiration int64

	if duration == 0 {
		// 0 表示使用默认过期时间
		duration = c.defaultExpiration
	}

	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.mu.Lock()
	c.items[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
	}
	c.mu.Unlock()
}

// Get 获取缓存项
func (c *MemoryCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	item, found := c.items[key]
	c.mu.RUnlock()

	// 如果未找到或已过期，返回未找到
	if !found || item.Expired() {
		c.recordMiss()
		return nil, false
	}

	c.recordHit()
	return item.Value, true
}

// recordHit 记录缓存命中
func (c *MemoryCache) recordHit() {
	c.hitCountMu.Lock()
	c.hitCount++
	c.hitCountMu.Unlock()
}

// recordMiss 记录缓存未命中
func (c *MemoryCache) recordMiss() {
	c.hitCountMu.Lock()
	c.missCount++
	c.hitCountMu.Unlock()
}

// Delete 删除缓存项
func (c *MemoryCache) Delete(key string) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Flush 清空所有缓存项
func (c *MemoryCache) Flush() {
	c.mu.Lock()
	c.items = make(map[string]CacheItem)
	c.mu.Unlock()
}

// startCleanupTimer 启动定时清理
func (c *MemoryCache) startCleanupTimer() {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-c.stopCleanup:
			return
		}
	}
}

// StopCleanup 停止定时清理
func (c *MemoryCache) StopCleanup() {
	c.stopCleanup <- true
}

// deleteExpired 删除过期项
func (c *MemoryCache) deleteExpired() {
	now := time.Now().UnixNano()

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.items {
		if v.Expiration > 0 && now > v.Expiration {
			delete(c.items, k)
		}
	}
}

// Stats 获取缓存统计信息
func (c *MemoryCache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// 获取命中率统计
	c.hitCountMu.RLock()
	hits := c.hitCount
	misses := c.missCount
	c.hitCountMu.RUnlock()

	// 计算命中率
	var hitRate float64
	totalRequests := hits + misses
	if totalRequests > 0 {
		hitRate = float64(hits) / float64(totalRequests) * 100
	}

	// 统计缓存项数量
	total := len(c.items)

	// 分析键类型统计
	typeStats := make(map[string]int)
	for key := range c.items {
		// 根据键前缀分类
		parts := strings.Split(key, ":")
		if len(parts) > 0 {
			prefix := parts[0]
			typeStats[prefix]++
		}
	}

	// 统计过期项
	now := time.Now().UnixNano()
	expired := 0
	for _, item := range c.items {
		if item.Expiration > 0 && now > item.Expiration {
			expired++
		}
	}

	return map[string]interface{}{
		"total":            total,
		"expired":          expired,
		"hit_count":        hits,
		"miss_count":       misses,
		"hit_rate":         hitRate,
		"type_stats":       typeStats,
		"memory_size":      "未计算", // 计算内存占用较复杂，暂不实现
		"cleanup_interval": c.cleanupInterval.String(),
	}
}
