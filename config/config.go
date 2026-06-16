package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port            int
	DataDir         string
	MaxMessageSize  int
	VisibilityTimeout time.Duration
	LongPollTimeout time.Duration
	HeartbeatTimeout time.Duration
	AdminToken      string
}

var AppConfig *Config

func Load() {
	port, _ := strconv.Atoi(getEnv("PORT", "8320"))
	maxMsgSize, _ := strconv.Atoi(getEnv("MAX_MESSAGE_SIZE", "1048576"))
	vt, _ := strconv.Atoi(getEnv("VISIBILITY_TIMEOUT", "60"))
	lp, _ := strconv.Atoi(getEnv("LONG_POLL_TIMEOUT", "30"))
	hb, _ := strconv.Atoi(getEnv("HEARTBEAT_TIMEOUT", "60"))

	AppConfig = &Config{
		Port:              port,
		DataDir:           getEnv("DATA_DIR", "./data"),
		MaxMessageSize:    maxMsgSize,
		VisibilityTimeout: time.Duration(vt) * time.Second,
		LongPollTimeout:   time.Duration(lp) * time.Second,
		HeartbeatTimeout:  time.Duration(hb) * time.Second,
		AdminToken:        getEnv("ADMIN_TOKEN", "admin123"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
