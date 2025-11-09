package db

import (
	"database/sql"
	"log"
	"os"
	"time"

	"go-kafka-pipeline/internal/metrics"
	"go-kafka-pipeline/internal/models"

	_ "github.com/microsoft/go-mssqldb" // ✅ Use MSSQL driver
)

// InitDB connects to MSSQL using connection string from .env
func InitDB() *sql.DB {
	connStr := os.Getenv("DB_CONN")
	if connStr == "" {
		log.Fatal("DB_CONN not set")
	}

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("❌ Failed to open DB: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("❌ Failed to connect to DB: %v", err)
	}

	log.Println("✅ Connected to MSSQL successfully")
	return db
}

// ------------------ UPSERT OPERATIONS ------------------
// MSSQL doesn’t have ON CONFLICT, use MERGE instead

func UpsertUser(db *sql.DB, e models.UserCreated) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	query := `
	MERGE users AS target
	USING (SELECT @p1 AS id, @p2 AS name, @p3 AS email) AS src
	ON (target.id = src.id)
	WHEN MATCHED THEN 
		UPDATE SET name = src.name, email = src.email
	WHEN NOT MATCHED THEN 
		INSERT (id, name, email) VALUES (src.id, src.name, src.email);`

	_, err := db.Exec(query, e.UserID, e.Name, e.Email)
	if err != nil {
		log.Println("UpsertUser error:", err)
	}
}

func UpsertOrder(db *sql.DB, e models.OrderPlaced) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	query := `
	MERGE orders AS target
	USING (SELECT @p1 AS id, @p2 AS userId, @p3 AS amount) AS src
	ON (target.id = src.id)
	WHEN MATCHED THEN 
		UPDATE SET userId = src.userId, amount = src.amount
	WHEN NOT MATCHED THEN 
		INSERT (id, userId, amount) VALUES (src.id, src.userId, src.amount);`

	_, err := db.Exec(query, e.OrderID, e.UserID, e.Amount)
	if err != nil {
		log.Println("UpsertOrder error:", err)
	}
}

func UpsertPayment(db *sql.DB, e models.PaymentSettled) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	query := `
	MERGE payments AS target
	USING (SELECT @p1 AS id, @p2 AS orderId, @p3 AS status) AS src
	ON (target.id = src.id)
	WHEN MATCHED THEN 
		UPDATE SET orderId = src.orderId, status = src.status
	WHEN NOT MATCHED THEN 
		INSERT (id, orderId, status) VALUES (src.id, src.orderId, src.status);`

	_, err := db.Exec(query, e.PaymentID, e.OrderID, e.Status)
	if err != nil {
		log.Println("UpsertPayment error:", err)
	}
}

func UpsertInventory(db *sql.DB, e models.InventoryAdjusted) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	query := `
	MERGE inventory AS target
	USING (SELECT @p1 AS sku, @p2 AS delta) AS src
	ON (target.sku = src.sku)
	WHEN MATCHED THEN 
		UPDATE SET delta = target.delta + src.delta
	WHEN NOT MATCHED THEN 
		INSERT (sku, delta) VALUES (src.sku, src.delta);`

	_, err := db.Exec(query, e.SKU, e.Delta)
	if err != nil {
		log.Println("UpsertInventory error:", err)
	}
}
