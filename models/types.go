package models

import (
	"math/rand"
	"time"
)

// 全局随机数生成器
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// Movie 电影模型
type Movie struct {
	MovieID   string   `json:"movieId"`
	Title     string   `json:"title"`
	Genres    []string `json:"genres"`
	Year      int      `json:"year,omitempty"`
	AvgRating float64  `json:"avgRating"`
	Links     Links    `json:"links,omitempty"`
	Tags      []string `json:"tags,omitempty"`
}

// Links 外部链接
type Links struct {
	ImdbID  string `json:"imdbId,omitempty"`
	ImdbURL string `json:"imdbUrl,omitempty"`
	TmdbID  string `json:"tmdbId,omitempty"`
	TmdbURL string `json:"tmdbUrl,omitempty"`
}

// MovieList 电影列表响应
type MovieList struct {
	Movies      []Movie `json:"movies"`
	TotalMovies int     `json:"totalMovies"`
	Page        int     `json:"page"`
	PerPage     int     `json:"perPage"`
	TotalPages  int     `json:"totalPages"`
}

// MovieDetail 电影详情响应
type MovieDetail struct {
	Movie       Movie               `json:"movie"`
	Ratings     []Rating            `json:"ratings,omitempty"`
	TaggedUsers []map[string]string `json:"taggedUsers,omitempty"`
	Stats       map[string]float64  `json:"stats,omitempty"`
}

// Rating 评分
type Rating struct {
	UserID string  `json:"userId"`
	Rating float64 `json:"rating"`
}
