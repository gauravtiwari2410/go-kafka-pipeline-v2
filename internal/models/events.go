package models

type UserCreated struct {
	EventID string `json:"eventId"`
	UserID  string `json:"userId"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Age     int    `json:"age"`
}

type OrderPlaced struct {
	EventID string  `json:"eventId"`
	OrderID string  `json:"orderId"`
	UserID  string  `json:"userId"`
	Amount  float64 `json:"amount"`
}

type PaymentSettled struct {
	EventID   string `json:"eventId"`
	PaymentID string `json:"paymentId"`
	OrderID   string `json:"orderId"`
	Status    string `json:"status"`
}

type InventoryAdjusted struct {
	EventID string `json:"eventId"`
	SKU     string `json:"sku"`
	Delta   int    `json:"delta"`
}
