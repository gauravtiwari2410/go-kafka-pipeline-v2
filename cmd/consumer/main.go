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
			log.Println("fetch error:", err)
			continue
		}

		log.Printf("fetched msg key=%s offset=%d", string(m.Key), m.Offset)

		var base map[string]interface{}
		if err := json.Unmarshal(m.Value, &base); err != nil {
			redis.PushToDLQ(ctx, string(m.Value), err.Error())
			reader.CommitMessages(ctx, m)
			continue
		}

		eventID := ""
		if v, ok := base["eventId"].(string); ok {
			eventID = v
		}

		log.Printf("processing eventId=%s key=%s offset=%d", eventID, string(m.Key), m.Offset)

		processed := false

		// route by shape
		if base["userId"] != nil && base["name"] != nil {
			var e models.UserCreated
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertUser(sqlDB, e)
				processed = true
			} else {
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}
		} else if base["orderId"] != nil && base["amount"] != nil {
			var e models.OrderPlaced
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertOrder(sqlDB, e)
				processed = true
			} else {
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}
		} else if base["paymentId"] != nil {
			var e models.PaymentSettled
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertPayment(sqlDB, e)
				processed = true
			} else {
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}
		} else if base["sku"] != nil {
			var e models.InventoryAdjusted
			if err := json.Unmarshal(m.Value, &e); err == nil {
				db.UpsertInventory(sqlDB, e)
				processed = true
			} else {
				redis.PushToDLQ(ctx, string(m.Value), err.Error())
			}
		} else {
			redis.PushToDLQ(ctx, string(m.Value), "unknown event shape")
		}

		if processed {
			metrics.MessagesProcessed.Inc()
			log.Printf("processed eventId=%s", eventID)
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error eventId=%s err=%v", eventID, err)
		}
	}
}
