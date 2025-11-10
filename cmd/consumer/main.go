package main

import (
	"context"
	"encoding/json"
	"log"

	"go-kafka-pipeline/internal/db"
	"go-kafka-pipeline/internal/kafka"
	"go-kafka-pipeline/internal/metrics"
	"go-kafka-pipeline/internal/models"
	"go-kafka-pipeline/internal/redis"
)

func main() {
	ctx := context.Background()
	reader := kafka.NewReader()
	defer reader.Close()

	sqlDB := db.InitDB()
	defer sqlDB.Close()

	log.Println("consumer started")

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("[ERROR] fetching message: %v", err)
			continue
		}

		log.Printf("[FETCHED] key=%s partition=%d offset=%d value=%s", string(m.Key), m.Partition, m.Offset, string(m.Value))

		var base map[string]interface{}
		if err := json.Unmarshal(m.Value, &base); err != nil {
			log.Printf("[DLQ] Invalid JSON, pushing to DLQ. Error: %v", err)
			redis.PushToDLQ(ctx, string(m.Value), err.Error())
			reader.CommitMessages(ctx, m)
			continue
		}

		eventID := ""
		if v, ok := base["eventId"].(string); ok {
			eventID = v
		}

		log.Printf("[PROCESSING] eventId=%s key=%s offset=%d baseKeys=%v", eventID, string(m.Key), m.Offset, getMapKeys(base))

		processed := false
		var eventType string

		// route by shape
		switch {
		case base["userId"] != nil && base["name"] != nil:
			eventType = "UserCreated"
			var e models.UserCreated
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertUser(sqlDB, e)
				processed = true
			} else {
				log.Printf("[DLQ] %s JSON unmarshal error: %v", eventType, err)
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}

		case base["orderId"] != nil && base["amount"] != nil:
			eventType = "OrderPlaced"
			var e models.OrderPlaced
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertOrder(sqlDB, e)
				processed = true
			} else {
				log.Printf("[DLQ] %s JSON unmarshal error: %v", eventType, err)
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}

		case base["paymentId"] != nil:
			eventType = "PaymentSettled"
			var e models.PaymentSettled
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertPayment(sqlDB, e)
				processed = true
			} else {
				log.Printf("[DLQ] %s JSON unmarshal error: %v", eventType, err)
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}

		case base["sku"] != nil:
			eventType = "InventoryAdjusted"
			var e models.InventoryAdjusted
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertInventory(sqlDB, e)
				processed = true
			} else {
				log.Printf("[DLQ] %s JSON unmarshal error: %v", eventType, err)
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}

		default:
			eventType = "Unknown"
			log.Printf("[DLQ] Unknown event shape. Keys: %v", getMapKeys(base))
			redis.PushToDLQ(ctx, string(m.Value), "unknown event shape")
		}

		if processed {
			metrics.MessagesProcessed.Inc()
			log.Printf("[PROCESSED] eventId=%s key=%s type=%s offset=%d", eventID, string(m.Key), eventType, m.Offset)
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("[COMMIT ERROR] eventId=%s type=%s offset=%d err=%v", eventID, eventType, m.Offset, err)
		}
	}
}

// helper function to print map keys for logging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
