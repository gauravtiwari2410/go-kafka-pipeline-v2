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

const (
	numUsers  = 10
	maxOrders = 10
)

func main() {
	rand.Seed(time.Now().UnixNano())
	log.Println("producer started")

	// Pre-generate user IDs
	userIDs := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		userIDs[i] = "U" + string(rune(i+1+48)) // U1, U2, ..., U10
	}

	// Track orders per user
	userOrderCount := make(map[string]int)

	for {
		// Pick random user
		userID := userIDs[rand.Intn(numUsers)]

		// Decide which event type to produce
		eventType := rand.Intn(4)
		var msg []byte
		var key string
		var eventName string
		eventID := randomID()

		switch eventType {
		case 0: // UserCreated
			e := models.UserCreated{
				EventID: eventID,
				UserID:  userID,
				Name:    "User-" + userID,
				Email:   "user" + userID + "@example.com",
			}
			msg, _ = json.Marshal(e)
			key = e.UserID
			eventName = "UserCreated"

		case 1: // OrderPlaced
			if userOrderCount[userID] >= maxOrders {
				continue // skip if user already has max orders
			}
			orderNum := userOrderCount[userID] + 1
			orderID := userID + "-O" + string(rune(orderNum+48))
			e := models.OrderPlaced{
				EventID: eventID,
				OrderID: orderID,
				UserID:  userID,
				Amount:  float64(rand.Intn(1000)) + rand.Float64(),
			}
			msg, _ = json.Marshal(e)
			key = e.OrderID
			eventName = "OrderPlaced"
			userOrderCount[userID]++

		case 2: // PaymentSettled
			if userOrderCount[userID] == 0 {
				continue // skip if user has no orders
			}
			orderIndex := rand.Intn(userOrderCount[userID]) + 1
			orderID := userID + "-O" + string(rune(orderIndex+48))
			e := models.PaymentSettled{
				EventID:   eventID,
				PaymentID: randomID(),
				OrderID:   orderID,
				Status:    "SUCCESS",
			}
			msg, _ = json.Marshal(e)
			key = e.OrderID
			eventName = "PaymentSettled"

		case 3: // InventoryAdjusted
			e := models.InventoryAdjusted{
				EventID: eventID,
				SKU:     "SKU" + string(rune(rand.Intn(10)+48)), // SKU0-SKU9
				Delta:   -rand.Intn(5) - 1,
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

		time.Sleep(500 * time.Millisecond) // faster production
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
