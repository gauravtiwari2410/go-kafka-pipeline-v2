package kafka

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
)

func NewWriter() *kafka.Writer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    "events",
		Balancer: &kafka.LeastBytes{},
	}
}

func ProduceMessage(ctx context.Context, key string, value []byte) error {
	w := NewWriter()
	defer w.Close()
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
	})
}
