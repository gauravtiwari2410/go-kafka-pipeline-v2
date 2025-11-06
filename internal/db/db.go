package db

import (
	"database/sql"
	"log"
	"os"
	"time"

	"go-kafka-pipeline/internal/models"
	"go-kafka-pipeline/internal/metrics"

	_ "github.com/microsoft/go-mssqldb"
)

func InitDB() *sql.DB {
	connStr := os.Getenv("DB_CONN")
	if connStr == "" {
		connStr = "sqlserver://sa:Password@123@mssql:1433?database=eventdb"
	}
	db, err := sql.Open("sqlserver", connStr)
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

	_, err := db.Exec(`IF EXISTS (SELECT 1 FROM users WHERE id=@p1)
		UPDATE users SET name=@p2,email=@p3 WHERE id=@p1
		ELSE
		INSERT INTO users(id,name,email) VALUES(@p1,@p2,@p3);`, e.UserID, e.Name, e.Email)
	if err != nil {
		log.Println("UpsertUser error:", err)
	}
}

func UpsertOrder(db *sql.DB, e models.OrderPlaced) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`IF EXISTS (SELECT 1 FROM orders WHERE id=@p1)
		UPDATE orders SET userId=@p2, amount=@p3 WHERE id=@p1
		ELSE
		INSERT INTO orders(id,userId,amount) VALUES(@p1,@p2,@p3);`, e.OrderID, e.UserID, e.Amount)
	if err != nil {
		log.Println("UpsertOrder error:", err)
	}
}

func UpsertPayment(db *sql.DB, e models.PaymentSettled) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`IF EXISTS (SELECT 1 FROM payments WHERE id=@p1)
		UPDATE payments SET orderId=@p2, status=@p3 WHERE id=@p1
		ELSE
		INSERT INTO payments(id,orderId,status) VALUES(@p1,@p2,@p3);`, e.PaymentID, e.OrderID, e.Status)
	if err != nil {
		log.Println("UpsertPayment error:", err)
	}
}

func UpsertInventory(db *sql.DB, e models.InventoryAdjusted) {
	start := time.Now()
	defer metrics.TrackDBLatency(start)

	_, err := db.Exec(`IF EXISTS (SELECT 1 FROM inventory WHERE sku=@p1)
		UPDATE inventory SET qty = qty + @p2 WHERE sku=@p1
		ELSE
		INSERT INTO inventory(sku,qty) VALUES(@p1,@p2);`, e.SKU, e.Delta)
	if err != nil {
		log.Println("UpsertInventory error:", err)
	}
}
