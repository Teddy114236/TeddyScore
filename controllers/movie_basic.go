package controllers

import (
	"gohbase/models"
	"gohbase/utils"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// GetMovies 获取电影列表
func (mc *MovieController) GetMovies(c *gin.Context) {
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

	// 获取电影列表
	movies, err := models.GetMoviesList(page, perPage)
	if err != nil {
		logrus.Errorf("获取电影列表失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影列表失败",
		})
		return
	}

	c.JSON(http.StatusOK, movies)
}

// GetMovie 获取电影详情
func (mc *MovieController) GetMovie(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "电影ID不能为空",
		})
		return
	}

	// 获取电影详情
	movie, err := models.GetMovieByID(movieID)
	if err != nil {
		logrus.Errorf("获取电影详情失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影详情失败",
		})
		return
	}

	// 如果电影不存在
	if movie == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "电影不存在",
		})
		return
	}

	c.JSON(http.StatusOK, movie)
}

// GetMovieRatings 获取电影的所有评分
func (mc *MovieController) GetMovieRatings(c *gin.Context) {
	// 获取电影ID
	movieID := c.Param("id")
	if movieID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "电影ID不能为空",
		})
		return
	}

	// 获取电影评分
	ratings, err := utils.GetMovieRatings(c.Request.Context(), movieID)
	if err != nil {
		logrus.Errorf("获取电影评分失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影评分失败",
		})
		return
	}

	// 如果评分不存在
	if ratings == nil {
		c.JSON(http.StatusOK, gin.H{
			"status":    "success",
			"ratings":   []interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"ratings":   ratings["ratings"],
		"count":     ratings["count"],
		"avgRating": ratings["avgRating"],
		"minRating": ratings["minRating"],
		"maxRating": ratings["maxRating"],
	})
}
