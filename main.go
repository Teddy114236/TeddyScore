package main

import (
	"context"
	"fmt"
	"gohbase/config"
	"gohbase/routes"
	"gohbase/utils"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	// 设置日志格式
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.InfoLevel)
}

func main() {
	cfg := config.GetConfig()

	logrus.Infof("配置信息: HBase主机=%s, ZooKeeper地址=%s, ZooKeeper端口=%s",
		cfg.HBase.Host, cfg.HBase.ZkQuorum, cfg.HBase.ZkPort)

	utils.InitCache(5*time.Minute, 10*time.Minute)
	logrus.Info("缓存系统初始化成功")

	err := utils.InitHBase(&cfg.HBase)
	if err != nil {
		logrus.Fatalf("初始化HBase失败: %v", err)
	}

	router := routes.SetupRouter()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Server.Port),
		Handler: router,
	}

	go func() {
		logrus.Infof("电影评分系统后端启动 [端口: %s]", cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("启动服务器失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logrus.Info("关闭服务器...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logrus.Fatalf("服务器强制关闭: %v", err)
	}

	logrus.Info("服务器已退出")
}
