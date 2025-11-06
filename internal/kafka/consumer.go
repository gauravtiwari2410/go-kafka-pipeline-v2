package kafka

import (
	"os"

	"github.com/segmentio/kafka-go"
)

func NewReader() *kafka.Reader {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "events",
		GroupID: "consumer-group-1",
	})
}
