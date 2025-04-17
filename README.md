<div align="center">

# ⚙ TeddyScore
#### **简体中文** | <a href="https://github.com/Teddy114236/TeddyScore/blob/main/README_EN.md"> English </a>

基于 Golang 的电影评分系统后端

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)
![GitHub stars](https://img.shields.io/github/stars/Teddy114236/TeddyScore?style=for-the-badge)
![GitHub issues](https://img.shields.io/github/issues/Teddy114236/TeddyScore?style=for-the-badge)
![GitHub pull requests](https://img.shields.io/github/issues-pr/Teddy114236/TeddyScore?style=for-the-badge)
![GitHub forks](https://img.shields.io/github/forks/Teddy114236/TeddyScore?style=for-the-badge)

</div>



## 📕 使用说明

使用  ``` go run main.go ``` 启动

默认运行在本机 5000 端口

### 接口信息
- `GET /api/movies` - 获取电影列表
- `GET /api/movies/:id` - 获取电影详情
- `GET /api/movies/random` - 获取随机电影
- `POST /api/movies/random` - 获取随机电影
- `GET /api/movies/search` - 搜索电影
- `GET /api/ratings/movie/:id` - 获取电影评分
- `GET /api/system/logs` - 获取系统日志
- `GET /api/system/cache` - 获取缓存统计信息 
