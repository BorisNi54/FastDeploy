package router

import (
	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	"github.com/PaddlePaddle/FastDeploy/router/internal/middleware"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/PaddlePaddle/FastDeploy/router/internal/gateway"
	"github.com/PaddlePaddle/FastDeploy/router/internal/manager"
)

func New(cfg *config.Config) *gin.Engine {
	// Set Gin mode
	gin.SetMode(cfg.Server.Mode)

	r := gin.New()

	// Global middleware
	r.Use(middleware.Logger())
	r.Use(middleware.Recovery())

	// API route group
	v1 := r.Group("/v1")
	{
		v1.POST("/chat/completions", gateway.ChatCompletions)
		v1.POST("/completions", gateway.Completions)
	}
	r.POST("/register", manager.RegisterInstance)
	r.GET("/registered_number", manager.RegisteredNumber)
	r.GET("/registered", manager.Registered)
	r.GET("/health", manager.Health)
	r.GET("/health_generate", manager.HealthGenerate)
	r.GET("/worker_metrics", manager.WorkerMetricsHandler)
	if cfg.Manager.EnableInferReadyCheck {
		r.GET("/ready", manager.InferReady)
	}
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	return r
}
