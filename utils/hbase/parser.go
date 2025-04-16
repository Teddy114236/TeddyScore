package hbase

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseMovieData 从HBase结果解析电影数据
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	result := map[string]interface{}{
		"movieId": movieID,
	}

	// 处理基本信息
	if movieData, ok := data["movie"]; ok {
		if title, ok := movieData["title"]; ok {
			result["title"] = string(title)
		}
		if genres, ok := movieData["genres"]; ok {
			result["genres"] = strings.Split(string(genres), "|")
		}
	}

	// 处理链接信息
	if linkData, ok := data["link"]; ok {
		links := map[string]interface{}{}

		if imdbId, ok := linkData["imdbId"]; ok {
			imdbIdStr := string(imdbId)
			links["imdbId"] = imdbIdStr
			links["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", imdbIdStr)
		}

		if tmdbId, ok := linkData["tmdbId"]; ok {
			tmdbIdStr := string(tmdbId)
			links["tmdbId"] = tmdbIdStr
			links["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", tmdbIdStr)
		}

		result["links"] = links
	} else {
		// 添加一个空的链接对象以避免前端错误
		result["links"] = map[string]interface{}{}
	}

	// 处理评分 - 修改为适配数据库的实际格式
	if ratingData, ok := data["rating"]; ok {
		var rating float64
		var timestamp int64
		var err error

		// 检查通用格式的评分
		if ratingValue, ok := ratingData["rating"]; ok {
			rating, err = strconv.ParseFloat(string(ratingValue), 64)
			if err == nil {
				// 处理评分时间戳
				if timestampValue, ok := ratingData["timestamp"]; ok {
					timestamp, _ = strconv.ParseInt(string(timestampValue), 10, 64)
				}

				// 构建评分信息
				ratings := []map[string]interface{}{
					{
						"userId":    "1", // 使用默认用户ID
						"rating":    rating,
						"timestamp": timestamp,
					},
				}
				result["ratings"] = ratings
				result["avgRating"] = rating // 单个评分时直接用它作为平均分
			}
		} else {
			// 兼容旧的复合列格式
			ratings := []map[string]interface{}{}
			ratingTimestamps := map[string]int64{}
			sumRating := 0.0
			count := 0

			// 先处理时间戳字段
			for column, value := range ratingData {
				if strings.HasPrefix(column, "timestamp:") {
					parts := strings.Split(column, ":")
					if len(parts) == 2 {
						userId := parts[1]
						timestamp, err := strconv.ParseInt(string(value), 10, 64)
						if err == nil {
							ratingTimestamps[userId] = timestamp
						}
					}
				}
			}

			// 处理评分字段
			for column, value := range ratingData {
				// 列名格式为 rating:{userId}
				if parts := strings.Split(column, ":"); len(parts) == 2 && parts[0] == "rating" && parts[1] != "rating" {
					userId := parts[1]
					rating, err := strconv.ParseFloat(string(value), 64)
					if err == nil {
						ratingInfo := map[string]interface{}{
							"userId": userId,
							"rating": rating,
						}

						// 添加时间戳如果存在
						if timestamp, ok := ratingTimestamps[userId]; ok {
							ratingInfo["timestamp"] = timestamp
						}

						ratings = append(ratings, ratingInfo)

						// 更新统计数据
						sumRating += rating
						count++
					}
				}
			}

			// 计算平均分
			if count > 0 {
				result["avgRating"] = sumRating / float64(count)
			} else {
				result["avgRating"] = 0.0
			}

			// 添加评分列表
			result["ratings"] = ratings
		}
	} else {
		result["avgRating"] = 0.0
		result["ratings"] = []interface{}{}
	}

	// 处理标签
	if tagData, ok := data["tag"]; ok {
		// 用于去重的映射
		uniqueTags := make(map[string]bool)

		// 处理标签字段
		for column, value := range tagData {
			// 只处理tag:前缀的列，而且格式为tag:{userId}
			if strings.HasPrefix(column, "tag:") {
				tagValue := string(value)
				if tagValue != "" {
					uniqueTags[tagValue] = true
				}
			}
		}

		// 转换为字符串数组
		tags := make([]string, 0, len(uniqueTags))
		for tag := range uniqueTags {
			tags = append(tags, tag)
		}

		result["uniqueTags"] = tags
	}

	return result
}
