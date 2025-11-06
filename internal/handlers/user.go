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
	err := db.QueryRow("SELECT id,name,email FROM users WHERE id=@p1", id).Scan(&user.ID, &user.Name, &user.Email)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "user not found"})
	}

	rows, _ := db.Query("SELECT TOP 5 id, amount FROM orders WHERE userId=@p1 ORDER BY id DESC", id)
	defer rows.Close()

	type Order struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
	}
	orders := []Order{}
	for rows.Next() {
		var o Order
		rows.Scan(&o.ID, &o.Amount)
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
	err := db.QueryRow("SELECT id,userId,amount FROM orders WHERE id=@p1", id).Scan(&order.ID, &order.UserID, &order.Amount)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "order not found"})
	}

	var status string
	_ = db.QueryRow("SELECT status FROM payments WHERE orderId=@p1", id).Scan(&status)

	return c.JSON(fiber.Map{"order": order, "paymentStatus": status})
}
