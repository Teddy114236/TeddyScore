package hbase

import (
	"context"
	"strconv"
)

// GetUserRating 获取用户对电影的评分
func GetUserRating(ctx context.Context, movieID string, userID string) (float64, int64, error) {
	// 获取电影评分
	families := []string{"rating"}
	data, err := GetMovieWithFamilies(ctx, movieID, families)
	if err != nil {
		return 0, 0, err
	}

	// 如果电影不存在
	if data == nil {
		return 0, 0, nil
	}

	// 查找用户评分
	if ratingData, ok := data["rating"]; ok {
		// 评分字段格式为 rating:{userId}
		ratingKey := "rating:" + userID
		if ratingValue, ok := ratingData[ratingKey]; ok {
			rating, err := strconv.ParseFloat(string(ratingValue), 64)
			if err != nil {
				return 0, 0, err
			}

			// 查找评分时间戳
			timestampKey := "timestamp:" + userID
			var timestamp int64
			if timestampValue, ok := ratingData[timestampKey]; ok {
				timestamp, _ = strconv.ParseInt(string(timestampValue), 10, 64)
			}

			return rating, timestamp, nil
		}
	}

	// 未找到用户评分
	return 0, 0, nil
}

// GetUserFavoriteGenres 获取用户最喜欢的电影类型
func GetUserFavoriteGenres(ctx context.Context, userID string) (map[string]int, error) {
	// 这里需要扫描用户的所有评分
	// 为简化实现，我们暂时返回空结果
	return map[string]int{}, nil
}

// GetUserTags 获取用户的标签
func GetUserTags(ctx context.Context, userID string) ([]string, error) {
	// 为简化实现，暂时返回空结果
	return []string{}, nil
}

// GetRecommendedMoviesForUser 获取推荐给用户的电影
func GetRecommendedMoviesForUser(ctx context.Context, userID string) ([]string, error) {
	// 为简化实现，暂时返回空结果
	return []string{}, nil
}
