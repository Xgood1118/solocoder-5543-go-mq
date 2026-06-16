package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/solomq/config"
	"github.com/solomq/internal/api"
	"github.com/solomq/internal/broker"
)

func main() {
	config.Load()

	log.Printf("🚀 SoloMQ starting on port %d...", config.AppConfig.Port)
	log.Printf("📁 Data directory: %s", config.AppConfig.DataDir)
	log.Printf("⏱️  Visibility timeout: %v", config.AppConfig.VisibilityTimeout)
	log.Printf("⏱️  Long poll timeout: %v", config.AppConfig.LongPollTimeout)
	log.Printf("⏱️  Heartbeat timeout: %v", config.AppConfig.HeartbeatTimeout)

	b, err := broker.NewBroker()
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.SetFuncMap(map[string]interface{}{
		"add": func(a, b int64) int64 { return a + b },
		"addIndex": func(a, b int) int { return a + b },
		"mul": func(a, b int) int { return a * b },
		"div": func(a, b int) float64 {
			if b == 0 {
				return 0
			}
			return float64(a) / float64(b)
		},
	})

	apiHandler := api.NewAPI(b)
	apiHandler.SetupRoutes(r)

	go startBackgroundTasks(b)

	addr := fmt.Sprintf(":%d", config.AppConfig.Port)
	log.Printf("✅ SoloMQ is ready. Admin UI: http://localhost:%d/admin?token=%s", config.AppConfig.Port, config.AppConfig.AdminToken)
	log.Printf("✅ Health check: http://localhost:%d/health", config.AppConfig.Port)

	if err := r.Run(addr); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func startBackgroundTasks(b *broker.Broker) {
	delayTicker := time.NewTicker(100 * time.Millisecond)
	visibilityTicker := time.NewTicker(5 * time.Second)
	heartbeatTicker := time.NewTicker(10 * time.Second)
	metricsTicker := time.NewTicker(10 * time.Second)
	cleanupTicker := time.NewTicker(1 * time.Hour)

	defer func() {
		delayTicker.Stop()
		visibilityTicker.Stop()
		heartbeatTicker.Stop()
		metricsTicker.Stop()
		cleanupTicker.Stop()
	}()

	for {
		select {
		case <-delayTicker.C:
			b.ProcessDelayQueue()
		case <-visibilityTicker.C:
			b.CheckVisibilityTimeouts()
		case <-heartbeatTicker.C:
			b.ProcessDeadInstances()
		case <-metricsTicker.C:
			b.RefreshMetrics()
		case <-cleanupTicker.C:
			b.CleanupExpired()
		}
	}
}
