package kafkaclient

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	writer *kafka.Writer
}

func NewKafkaClient(brokers []string, topic string) *KafkaClient {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	return &KafkaClient{writer: writer}
}

func (c KafkaClient) Send(eventBytes []byte) error {
	msg := kafka.Message{
		Key:   []byte("osmosis-pool-liquidities"),
		Value: eventBytes,
		Time:  time.Now(),
	}
	if err := c.writer.WriteMessages(context.Background(), msg); err != nil {
		return err
	}
	return nil
}
