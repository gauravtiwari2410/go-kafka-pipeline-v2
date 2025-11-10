package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"go-kafka-pipeline/internal/kafka"
	"go-kafka-pipeline/internal/models"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.Println("producer started")
	for i := 0; ; i++ {
		eventType := rand.Intn(4)
		var msg []byte
		var key string
		var eventName string // New: store event type for logging

		switch eventType {
		case 0:
			e := models.UserCreated{
				EventID: randomID(),
				UserID:  "U1",
				Name:    "Gaurav",
				Email:   "gaurav@example.com",
			}
			msg, _ = json.Marshal(e)
			key = e.UserID
			eventName = "UserCreated"
		case 1:
			e := models.OrderPlaced{
				EventID: randomID(),
				OrderID: "O1",
				UserID:  "U1",
				Amount:  250.5,
			}
			msg, _ = json.Marshal(e)
			key = e.OrderID
			eventName = "OrderPlaced"
		case 2:
			e := models.PaymentSettled{
				EventID:   randomID(),
				PaymentID: randomID(),
				OrderID:   "O1",
				Status:    "SUCCESS",
			}
			msg, _ = json.Marshal(e)
			key = e.OrderID
			eventName = "PaymentSettled"
		case 3:
			e := models.InventoryAdjusted{
				EventID: randomID(),
				SKU:     "SKU123",
				Delta:   -2,
			}
			msg, _ = json.Marshal(e)
			key = e.SKU
			eventName = "InventoryAdjusted"
		}

		if err := kafka.ProduceMessage(context.Background(), key, msg); err != nil {
			log.Printf("[ERROR] produce_error type=%s eventId=%s key=%s err=%v", eventName, extractEventID(msg), key, err)
		} else {
			log.Printf("[INFO] produced event type=%s eventId=%s key=%s", eventName, extractEventID(msg), key)
		}

		time.Sleep(2 * time.Second)
	}
}

// randomID generates a timestamp-based unique ID
func randomID() string {
	return time.Now().Format("20060102T150405.000") + "-" + string(rune(65+rand.Intn(26)))
}

// extractEventID extracts EventID from a JSON message
func extractEventID(b []byte) string {
	type base struct {
		EventID string `json:"eventId"`
	}
	var x base
	_ = json.Unmarshal(b, &x)
	return x.EventID
}
