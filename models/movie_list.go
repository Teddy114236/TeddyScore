package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"strconv"
	"strings"
)

// GetTotalMoviesCount 获取电影总数
func GetTotalMoviesCount(ctx context.Context) (int, error) {
	// 使用缓存优化性能
	cacheKey := "total_movies_count"
	if cachedCount, found := utils.Cache.Get(cacheKey); found {
		return cachedCount.(int), nil
	}

	// 使用 ScanMoviesWithPagination，它会返回总数，而不直接使用客户端
	_, totalCount, err := utils.ScanMoviesWithPagination(ctx, 1, 1)
	if err != nil {
		return 0, err // 出错时返回0和错误，而不是硬编码值
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, totalCount)

	return totalCount, nil
}

// GetMoviesList 获取电影列表
func GetMoviesList(page, perPage int) (*MovieList, error) {
	ctx := context.Background()

	// 获取总电影数
	totalMovies, err := GetTotalMoviesCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取电影总数失败: %w", err) // 如果获取失败，返回错误而不是使用默认值
	}

	// 计算分页参数
	startIdx := (page-1)*perPage + 1 // 从1开始
	endIdx := startIdx + perPage

	// 扫描电影范围
	startRow := fmt.Sprintf("%d", startIdx)
	endRow := fmt.Sprintf("%d", endIdx)

	// 直接使用 ScanMovies 而非先获取客户端再扫描
	results, err := utils.ScanMovies(ctx, startRow, endRow, int64(perPage))
	if err != nil {
		return nil, err
	}

	// 解析电影列表
	movies := []Movie{}

	for _, result := range results {
		// 获取行键（即movieId）
		var movieID string
		for _, cell := range result.Cells {
			movieID = string(cell.Row)
			break
		}

		if movieID == "" {
			continue
		}

		// 手动构建结果映射
		resultMap := make(map[string]map[string][]byte)
		for _, cell := range result.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if _, ok := resultMap[family]; !ok {
				resultMap[family] = make(map[string][]byte)
			}

			resultMap[family][qualifier] = cell.Value
		}

		movieData := utils.ParseMovieData(movieID, resultMap)

		movie := Movie{
			MovieID: movieID,
		}

		if title, ok := movieData["title"].(string); ok {
			movie.Title = title
			// 尝试从标题中提取年份
			if matches := strings.Split(title, " ("); len(matches) > 1 {
				yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
				if year, err := strconv.Atoi(yearStr); err == nil {
					movie.Year = year
				}
			}
		}

		if genres, ok := movieData["genres"].([]string); ok {
			movie.Genres = genres
		}

		if avgRating, ok := movieData["avgRating"].(float64); ok {
			movie.AvgRating = avgRating
		}

		// 添加链接数据
		if links, ok := movieData["links"].(map[string]interface{}); ok {
			linkObj := Links{}

			if imdbId, ok := links["imdbId"].(string); ok {
				linkObj.ImdbID = imdbId
			}
			if imdbUrl, ok := links["imdbUrl"].(string); ok {
				linkObj.ImdbURL = imdbUrl
			}
			if tmdbId, ok := links["tmdbId"].(string); ok {
				linkObj.TmdbID = tmdbId
			}
			if tmdbUrl, ok := links["tmdbUrl"].(string); ok {
				linkObj.TmdbURL = tmdbUrl
			}

			movie.Links = linkObj
		}

		// 添加标签数据
		if uniqueTags, ok := movieData["uniqueTags"].([]string); ok {
			movie.Tags = uniqueTags
		}

		movies = append(movies, movie)
	}

	// 构建响应
	totalPages := (totalMovies + perPage - 1) / perPage // 计算总页数

	return &MovieList{
		Movies:      movies,
		TotalMovies: totalMovies,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}, nil
}
