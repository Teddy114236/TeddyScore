package controllers

import (
	"gohbase/models"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// SearchMovies 搜索电影
func (mc *MovieController) SearchMovies(c *gin.Context) {
	// 获取查询参数
	query := c.Query("query")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "搜索关键词不能为空",
		})
		return
	}

	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	perPageStr := c.DefaultQuery("per_page", "12")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	perPage, err := strconv.Atoi(perPageStr)
	if err != nil || perPage < 1 {
		perPage = 12
	}

	// 限制每页最大数量为50
	if perPage > 50 {
		perPage = 50
	}

	// 搜索电影
	result, err := models.SearchMovies(query, page, perPage)
	if err != nil {
		logrus.Errorf("搜索电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "搜索电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}
