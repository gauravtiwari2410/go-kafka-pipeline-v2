package main

import (
	"log"
	"os"

	"go-kafka-pipeline/internal/db"
	"go-kafka-pipeline/internal/handlers"
	"go-kafka-pipeline/internal/metrics"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	app := fiber.New()
	sqlDB := db.InitDB()
	defer sqlDB.Close()

	app.Get("/users", func(c *fiber.Ctx) error {
		return handlers.GetTotalUser(c, sqlDB)
	})

	app.Get("/users/:id", func(c *fiber.Ctx) error {
		return handlers.GetUserWithOrders(c, sqlDB)
	})
	app.Get("/orders/:id", func(c *fiber.Ctx) error {
		return handlers.GetOrderWithPayment(c, sqlDB)
	})

	// metrics endpoint
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("api listening on :" + port)
	// expose metrics for Prometheus (already hooked via global vars in metrics package)
	_ = metrics.Init() // ensure metrics initialized
	app.Listen(":" + port)
}
