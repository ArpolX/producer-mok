package kafka

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
}

func NewKafkaProducer(address string, topic string) (*Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":   address,
		"go.delivery.reports": true,
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("failed to create producer in NewKafkaProducer: %v", err)
		return nil, fmt.Errorf("failed to create producer in NewKafkaProducer: %w", err)
	}

	log.Print("producer initialized")

	return &Producer{
		producer: p,
		topic:    topic,
	}, nil
}

func (p *Producer) SendMessage(message []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          message,
	}

	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := p.producer.Produce(msg, deliveryChan)
	if err != nil {
		log.Printf("failed to produce message in SendMessage: %v", err)
		return fmt.Errorf("failed to send message in SendMessage: %w", err)
	}

	e := <-deliveryChan
	m, ok := e.(*kafka.Message)
	if !ok || m.TopicPartition.Error != nil {
		log.Printf("failed to deliver message in SendMessage: %v", m.TopicPartition.Error)
		return fmt.Errorf("failed to send message in SendMessage: %w", m.TopicPartition.Error)
	}

	log.Print("message sent in SendMessage")
	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
	log.Print("producer closed in Close")
}
