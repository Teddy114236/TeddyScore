package hbase

import (
	"context"
	"fmt"
)

// GetMoviesMultiple 根据多个ID获取电影信息
func GetMoviesMultiple(ctx context.Context, movieIDs []string) (map[string]map[string]map[string][]byte, error) {
	results := make(map[string]map[string]map[string][]byte)

	// 使用goroutine并发获取多部电影信息
	type result struct {
		id   string
		data map[string]map[string][]byte
		err  error
	}

	resultChan := make(chan result, len(movieIDs))

	for _, id := range movieIDs {
		go func(movieID string) {
			data, err := GetMovie(ctx, movieID)
			resultChan <- result{id: movieID, data: data, err: err}
		}(id)
	}

	// 收集结果
	for range movieIDs {
		res := <-resultChan
		if res.err == nil && res.data != nil {
			results[res.id] = res.data
		}
	}

	return results, nil
}

// GetMovieRatingStats 获取电影评分统计
func GetMovieRatingStats(ctx context.Context, movieID string) (map[string]float64, error) {
	// 获取所有评分
	families := []string{"rating"}
	result, err := GetMovieWithFamilies(ctx, movieID, families)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return map[string]float64{
			"avgRating": 0,
			"minRating": 0,
			"maxRating": 0,
			"count":     0,
		}, nil
	}

	// 计算统计数据
	var count, sum, min, max float64 = 0, 0, 5, 0

	if ratingData, ok := result["rating"]; ok {
		for column, value := range ratingData {
			// 只处理评分字段
			if len(column) > 7 && column[:7] == "rating:" {
				rating := parseFloat(string(value), 0)
				if rating > 0 {
					sum += rating
					count++

					if rating < min {
						min = rating
					}
					if rating > max {
						max = rating
					}
				}
			}
		}
	}

	// 计算平均分
	avgRating := 0.0
	if count > 0 {
		avgRating = sum / count
	}

	// 如果没有评分，设置最小最大值为0
	if count == 0 {
		min = 0
	}

	return map[string]float64{
		"avgRating": avgRating,
		"minRating": min,
		"maxRating": max,
		"count":     count,
	}, nil
}

// 解析浮点数，出错时返回默认值
func parseFloat(s string, defaultValue float64) float64 {
	var v float64
	_, err := fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return defaultValue
	}
	return v
}
