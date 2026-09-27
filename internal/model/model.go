package model

import "time"

type User struct {
	ID           int64     `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

type Message struct {
	ID              int64      `json:"id"`
	ClientMessageID string     `json:"client_message_id"`
	SenderID        int64      `json:"sender_id"`
	RecipientID     int64      `json:"recipient_id"`
	Body            string     `json:"body"`
	CreatedAt       time.Time  `json:"created_at"`
	DeliveryStatus  string     `json:"delivery_status"`
	DeliveredAt     *time.Time `json:"delivered_at,omitempty"`
}
