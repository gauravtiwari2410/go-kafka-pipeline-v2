package handlers

import (
	"database/sql"

	"github.com/gofiber/fiber/v2"
)

func GetUserWithOrders(c *fiber.Ctx, db *sql.DB) error {
	id := c.Params("id")

	var user struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	// ✅ Use @p1 instead of $1 for MSSQL
	err := db.QueryRow("SELECT id, name, email FROM users WHERE id = @p1", id).Scan(&user.ID, &user.Name, &user.Email)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "user not found"})
	}

	// ✅ MSSQL doesn't support LIMIT → use TOP instead
	rows, err := db.Query("SELECT TOP 5 id, amount FROM orders WHERE userId = @p1 ORDER BY id DESC", id)
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
			continue
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

	// ✅ MSSQL parameter format
	err := db.QueryRow("SELECT id, userId, amount FROM orders WHERE id = @p1", id).Scan(&order.ID, &order.UserID, &order.Amount)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "order not found"})
	}

	var status string
	err = db.QueryRow("SELECT status FROM payments WHERE order_id = @p1", id).Scan(&status)
	if err != nil {
		status = "not found"
	}

	return c.JSON(fiber.Map{"order": order, "paymentStatus": status})
}
