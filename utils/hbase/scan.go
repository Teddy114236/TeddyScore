package hbase

import (
	"context"
	"strings"

	"github.com/tsuna/gohbase/hrpc"
)

// ScanMovies 扫描电影
func ScanMovies(ctx context.Context, startRow, endRow string, limit int64) ([]*hrpc.Result, error) {
	// 构建Scan对象
	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, endRow)
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}
		results = append(results, result)
		count++
	}

	return results, nil
}

// ScanMoviesWithFamilies 使用指定列族扫描电影
func ScanMoviesWithFamilies(ctx context.Context, startRow, endRow string, families []string, limit int64) ([]*hrpc.Result, error) {
	// 构建列族映射
	familiesMap := make(map[string][]string)
	for _, family := range families {
		familiesMap[family] = nil
	}

	// 构建Scan对象，并指定列族
	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, endRow, hrpc.Families(familiesMap))
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}
		results = append(results, result)
		count++
	}

	return results, nil
}

// ScanMoviesByGenre 根据电影类型扫描电影
func ScanMoviesByGenre(ctx context.Context, genre string, limit int64) ([]*hrpc.Result, error) {
	// 简化为基本扫描，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果并筛选包含指定类型的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 检查这个结果是否包含指定的类型
		for _, cell := range result.Cells {
			if string(cell.Family) == "movie" && string(cell.Qualifier) == "genres" {
				genreValue := string(cell.Value)
				if strings.Contains(strings.ToLower(genreValue), strings.ToLower(genre)) {
					results = append(results, result)
					count++
					break
				}
			}
		}
	}

	return results, nil
}

// ScanMoviesByTag 根据标签扫描电影
func ScanMoviesByTag(ctx context.Context, tag string, limit int64) ([]*hrpc.Result, error) {
	// 简化为基本扫描，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果并筛选包含指定标签的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 检查这个结果是否包含指定的标签
		hasTag := false
		for _, cell := range result.Cells {
			if string(cell.Family) == "tag" && strings.HasPrefix(string(cell.Qualifier), "tag:") {
				tagValue := string(cell.Value)
				if strings.Contains(strings.ToLower(tagValue), strings.ToLower(tag)) {
					hasTag = true
					break
				}
			}
		}

		if hasTag {
			results = append(results, result)
			count++
		}
	}

	return results, nil
}

// ScanMoviesWithPagination 带分页的电影扫描
func ScanMoviesWithPagination(ctx context.Context, page, pageSize int) ([]*hrpc.Result, int, error) {
	// 计算扫描范围
	startRow := "1" // 第一行
	totalRows := 0

	// 构建扫描请求
	scanRequest, err := hrpc.NewScanRangeStr(ctx, "moviedata", startRow, "")
	if err != nil {
		return nil, 0, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var allResults []*hrpc.Result

	// 收集所有结果（注意：在实际应用中，这种方式可能不适用于大数据集）
	for {
		result, err := scanner.Next()
		if err != nil {
			break
		}
		allResults = append(allResults, result)
	}

	// 计算总行数
	totalRows = len(allResults)

	// 计算分页
	startIndex := (page - 1) * pageSize
	endIndex := startIndex + pageSize
	if endIndex > totalRows {
		endIndex = totalRows
	}

	// 如果起始索引超出范围
	if startIndex >= totalRows {
		return []*hrpc.Result{}, totalRows, nil
	}

	// 返回分页结果
	return allResults[startIndex:endIndex], totalRows, nil
}

// SearchMovies 搜索电影
func SearchMovies(ctx context.Context, query string, limit int64) ([]*hrpc.Result, error) {
	query = strings.ToLower(query)

	// 使用简单扫描，然后在应用层做过滤
	scanRequest, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return nil, err
	}

	// 执行扫描
	scanner := hbaseClient.Scan(scanRequest)
	var results []*hrpc.Result
	count := int64(0)

	// 收集结果并筛选匹配查询的电影
	for count < limit {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		// 检查标题和类型是否匹配查询
		isMatch := false
		for _, cell := range result.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)
			value := string(cell.Value)

			if family == "movie" && (qualifier == "title" || qualifier == "genres") {
				if strings.Contains(strings.ToLower(value), query) {
					isMatch = true
					break
				}
			}
		}

		if isMatch {
			results = append(results, result)
			count++
		}
	}

	return results, nil
}
