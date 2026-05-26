package main

import (
	"kafka-producer/internal/infrastructure/kafka"
	"kafka-producer/internal/worker"
	"log"
	"time"
)

const (
	topic        = "my-topic"
	kafkaAddress = "localhost:9092"
)

func main() {
	producer, err := kafka.NewKafkaProducer(kafkaAddress, topic)
	if err != nil {
		log.Fatalf("error start producer %v", err)
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		req := worker.Worker()

		if err := producer.SendMessage(req); err != nil {
			log.Fatalf("error send message %v", err)
			return
		}
	}
}
