package routes

import (
	"gohbase/controllers"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// SetupRouter 设置路由
func SetupRouter() *gin.Engine {
	// 创建默认路由
	router := gin.Default()

	// 添加CORS中间件，允许所有来源、方法和头部
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Cache-Check", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "X-Cache-Hit"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// 创建API路由组
	api := router.Group("/api")

	// 创建控制器实例
	movieController := &controllers.MovieController{}

	// 电影相关路由
	movies := api.Group("/movies")
	{
		movies.GET("", movieController.GetMovies)
		movies.GET("/:id", movieController.GetMovie)
		movies.GET("/random", movieController.GetRandomMovies)
		movies.POST("/random", movieController.RandomMoviesPost)
		movies.GET("/search", movieController.SearchMovies)
	}

	// 评分相关路由
	ratings := api.Group("/ratings")
	{
		ratings.GET("/movie/:id", movieController.GetMovieRatings)
	}

	// 系统日志路由
	// GET /api/system/logs - 获取系统日志
	api.GET("/system/logs", movieController.GetSystemLogs)

	// 添加缓存统计路由
	// GET /api/system/cache - 获取缓存统计信息
	api.GET("/system/cache", movieController.GetCacheStats)

	// 返回路由
	return router
}
