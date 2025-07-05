package internal

import (
	"os"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	GRPCPort string
	DBPath   string
}

func LoadConfig() Config {
	port := os.Getenv("ARROW_RECEIVER_GRPC_PORT")
	if port == "" {
		port = ":9002"
	}
	dbPath := os.Getenv("ARROW_RECEIVER_DB_PATH")
	if dbPath == "" {
		dbPath = "traces.db"
	}
	return Config{
		GRPCPort: port,
		DBPath:   dbPath,
	}
}

func SetupLogger() {
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(log.InfoLevel)
} 