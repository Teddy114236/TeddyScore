package controllers

import (
	"gohbase/models"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// GetRandomMovies 获取随机电影
func (mc *MovieController) GetRandomMovies(c *gin.Context) {
	// 获取数量参数
	countStr := c.DefaultQuery("count", "6")
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		count = 6
	}

	// 限制最大数量为20
	if count > 20 {
		count = 20
	}

	// 获取随机电影
	movies, err := models.GetRandomMovies(count)
	if err != nil {
		logrus.Errorf("获取随机电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取随机电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"movies": movies,
	})
}

// RandomMoviesPost 获取随机电影（POST方法，兼容不支持查询参数的客户端）
func (mc *MovieController) RandomMoviesPost(c *gin.Context) {
	var request struct {
		Count int `json:"count"`
	}

	if err := c.BindJSON(&request); err != nil {
		request.Count = 6
	}

	// 限制数量
	if request.Count < 1 {
		request.Count = 6
	}
	if request.Count > 20 {
		request.Count = 20
	}

	// 获取随机电影
	movies, err := models.GetRandomMovies(request.Count)
	if err != nil {
		logrus.Errorf("获取随机电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取随机电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"movies": movies,
	})
}
