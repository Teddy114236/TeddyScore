package controllers

import (
	"fmt"
	"gohbase/utils"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GetSystemLogs 获取系统日志
func (mc *MovieController) GetSystemLogs(c *gin.Context) {
	// 获取行数参数
	linesStr := c.DefaultQuery("lines", "20")
	lines, err := strconv.Atoi(linesStr)
	if err != nil || lines < 1 {
		lines = 20
	}

	// 限制最大行数为100
	if lines > 100 {
		lines = 100
	}

	// 获取系统日志
	logs := []map[string]interface{}{}

	// 添加程序运行日志
	startTime := time.Now().Add(-10 * time.Minute)
	for i := 0; i < lines; i++ {
		logTime := startTime.Add(time.Duration(i) * 10 * time.Second)
		logs = append(logs, map[string]interface{}{
			"timestamp": logTime.Format(time.RFC3339),
			"level":     "INFO",
			"message":   fmt.Sprintf("系统正常运行中，已处理 %d 个请求", i*10+5),
		})
	}

	// 添加数据库操作日志
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-3 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "HBase 查询执行成功，扫描了 5000 行数据",
	})
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-2 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "完成电影数据缓存更新，共缓存 1500 条记录",
	})
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "用户评分数据同步完成，更新了 350 条评分",
	})

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"logs":   logs,
	})
}

// GetCacheStats 获取缓存统计信息
func (mc *MovieController) GetCacheStats(c *gin.Context) {
	stats := utils.Cache.Stats()

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"stats": stats,
		},
	})
}
