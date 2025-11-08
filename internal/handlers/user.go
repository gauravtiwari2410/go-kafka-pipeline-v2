package handlers

import (
	"database/sql"

	"github.com/gofiber/fiber/v2"
)

func GetUserWithOrders(c *fiber.Ctx, db *sql.DB) error {
	id := c.Params("id")

	// User struct matches DB columns
	var user struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	// Query users table
	err := db.QueryRow("SELECT user_id, name, email FROM users WHERE user_id=$1", id).Scan(&user.ID, &user.Name, &user.Email)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "user not found"})
	}

	// Query last 5 orders for this user
	rows, err := db.Query("SELECT order_id, amount FROM orders WHERE user_id=$1 ORDER BY order_id DESC LIMIT 5", id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "failed to fetch orders"})
	}
	defer rows.Close()

	type Order struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
	}

	var orders []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.Amount); err != nil {
			continue // skip if error in this row
		}
		orders = append(orders, o)
	}

	return c.JSON(fiber.Map{"user": user, "lastOrders": orders})
}

func GetOrderWithPayment(c *fiber.Ctx, db *sql.DB) error {
	id := c.Params("id")

	var order struct {
		ID     string  `json:"id"`
		UserID string  `json:"userId"`
		Amount float64 `json:"amount"`
	}

	// Query orders table
	err := db.QueryRow("SELECT order_id, user_id, amount FROM orders WHERE order_id=$1", id).Scan(&order.ID, &order.UserID, &order.Amount)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "order not found"})
	}

	// Query payment status
	var status string
	err = db.QueryRow("SELECT status FROM payments WHERE order_id=$1", id).Scan(&status)
	if err != nil {
		status = "not found" // if payment does not exist
	}

	return c.JSON(fiber.Map{"order": order, "paymentStatus": status})
}
