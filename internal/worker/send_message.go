package worker

import (
	"encoding/json"
	"kafka-producer/internal/dto"
	"strconv"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

func Worker() []byte {
	faker := gofakeit.New(gofakeit.Uint64())

	message := dto.Event{
		Message:   faker.School(),
		UserId:    strconv.Itoa(faker.Int()),
		Timestamp: time.Now(),
	}

	req, _ := json.Marshal(&message)
	return req
}
