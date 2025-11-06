package redis

import (
	"context"
	"log"
	"os"

	"go-kafka-pipeline/internal/metrics"

	"github.com/go-redis/redis/v8"
)

var rdb *redis.Client

func init() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}
	rdb = redis.NewClient(&redis.Options{
		Addr: addr,
	})
}

func PushToDLQ(ctx context.Context, payload, reason string) {
	entry := reason + " | " + payload
	if err := rdb.LPush(ctx, "dlq", entry).Err(); err != nil {
		log.Println("dlq push error:", err)
	}
	metrics.DLQCount.Inc()
}
