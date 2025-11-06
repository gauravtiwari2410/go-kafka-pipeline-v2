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

		switch eventType {
		case 0:
			e := models.UserCreated{EventID: randomID(), UserID: "U1", Name: "Gaurav", Email: "gaurav@example.com"}
			msg, _ = json.Marshal(e)
			key = e.UserID
		case 1:
			e := models.OrderPlaced{EventID: randomID(), OrderID: "O1", UserID: "U1", Amount: 250.5}
			msg, _ = json.Marshal(e)
			key = e.OrderID
		case 2:
			e := models.PaymentSettled{EventID: randomID(), PaymentID: randomID(), OrderID: "O1", Status: "SUCCESS"}
			msg, _ = json.Marshal(e)
			key = e.OrderID
		case 3:
			e := models.InventoryAdjusted{EventID: randomID(), SKU: "SKU123", Delta: -2}
			msg, _ = json.Marshal(e)
			key = e.SKU
		}

		if err := kafka.ProduceMessage(context.Background(), key, msg); err != nil {
			log.Printf("produce_error eventId=%s key=%s err=%v", extractEventID(msg), key, err)
		} else {
			log.Printf("produced event eventId=%s key=%s", extractEventID(msg), key)
		}

		time.Sleep(2 * time.Second)
	}
}

func randomID() string {
	return time.Now().Format("20060102T150405.000") + "-" + string(rune(65+rand.Intn(26)))
}

func extractEventID(b []byte) string {
	type base struct {
		EventID string `json:"eventId"`
	}
	var x base
	_ = json.Unmarshal(b, &x)
	return x.EventID
}
