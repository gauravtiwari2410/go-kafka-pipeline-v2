package db

import (
	"database/sql"
	"log"
	"os"
	"time"

	"go-kafka-pipeline/internal/metrics"
	"go-kafka-pipeline/internal/models"

	_ "github.com/lib/pq" // PostgreSQL driver
)

func InitDB() *sql.DB {
	connStr := os.Getenv("DB_CONN")
	if connStr == "" {
		connStr = "postgres://postgres:root@postgres:5432/eventdb?sslmode=disable"
	}
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("db open error:", err)
	}
	if err := db.Ping(); err != nil {
		log.Println("db ping warning (may be ready later):", err)
	}
	return db
}

func UpsertUser(db *sql.DB, e models.UserCreated) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`INSERT INTO users(id, name, email)
		VALUES($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;`,
		e.UserID, e.Name, e.Email)
	if err != nil {
		log.Println("UpsertUser error:", err)
	}
}

func UpsertOrder(db *sql.DB, e models.OrderPlaced) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`INSERT INTO orders(id, userId, amount)
		VALUES($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET userId = EXCLUDED.userId, amount = EXCLUDED.amount;`,
		e.OrderID, e.UserID, e.Amount)
	if err != nil {
		log.Println("UpsertOrder error:", err)
	}
}

func UpsertPayment(db *sql.DB, e models.PaymentSettled) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`INSERT INTO payments(id, orderId, status)
		VALUES($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET orderId = EXCLUDED.orderId, status = EXCLUDED.status;`,
		e.PaymentID, e.OrderID, e.Status)
	if err != nil {
		log.Println("UpsertPayment error:", err)
	}
}

func UpsertInventory(db *sql.DB, e models.InventoryAdjusted) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`INSERT INTO inventory(sku, qty)
		VALUES($1, $2)
		ON CONFLICT (sku) DO UPDATE SET qty = inventory.qty + EXCLUDED.qty;`,
		e.SKU, e.Delta)
	if err != nil {
		log.Println("UpsertInventory error:", err)
	}
}
