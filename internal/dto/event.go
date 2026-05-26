package dto

import "time"

type Event struct {
	Message   string    `json:"message"`
	UserId    string    `json:"user_id"`
	Timestamp time.Time `json:"timestamp"`
}
