package events

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type kafkaQueue struct {
	config KafkaQueueConfig
	w      *kafka.Writer
	wOnce  sync.Once
	r      *kafka.Reader
	rOnce  sync.Once
}

type KafkaQueueConfig struct {
	Brokers      []string
	Topic        string
	ConsumerName string
}

func KafkaQueue(config KafkaQueueConfig) EventQueue {
	return &kafkaQueue{config: config}
}

func (q *kafkaQueue) Name() string {
	return "kafka-" + q.config.Topic + "-" + q.config.ConsumerName
}

func (q *kafkaQueue) Add(ctx context.Context, e *Event) (err error) {
	q.wOnce.Do(func() {
		q.w = &kafka.Writer{
			Addr:  kafka.TCP(q.config.Brokers...),
			Topic: q.config.Topic,
		}
	})
	msg := kafka.Message{
		Key:     []byte(e.ID),
		Value:   e.Data,
		Headers: make([]kafka.Header, len(e.Metadata)+1),
	}
	msg.Headers[0] = kafka.Header{Key: "__type__", Value: []byte(e.Type)}
	var i int = 1
	for k, v := range e.Metadata {
		msg.Headers[i] = kafka.Header{Key: k, Value: []byte(v)}
	}
	if err := q.w.WriteMessages(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (q *kafkaQueue) Next(ctx context.Context, e *Event) (err error) {
	q.rOnce.Do(func() {
		q.r = kafka.NewReader(kafka.ReaderConfig{
			Brokers: q.config.Brokers,
			Topic:   q.config.Topic,
			GroupID: q.config.ConsumerName,
		})
	})
	var msg kafka.Message
	if msg, err = q.r.ReadMessage(ctx); err != nil {
		return
	}
	e.Metadata = make(map[string]string)
	for _, h := range msg.Headers {
		val := make([]byte, len(h.Value))
		copy(val, h.Value)
		if h.Key == "__type__" {
			e.Type = string(val)
			continue
		}
		e.Metadata[h.Key] = string(val)
	}
	e.Data = make([]byte, len(msg.Value))
	copy(e.Data, msg.Value)
	key := make([]byte, len(msg.Key))
	copy(key, msg.Key)
	e.ID = string(key)
	err = q.r.CommitMessages(ctx, msg)
	return
}

func (q *kafkaQueue) Close() error {
	if q.r != nil {
		if err := q.r.Close(); err != nil {
			return err
		}
		q.rOnce = sync.Once{}
	}
	if q.w != nil {
		if err := q.w.Close(); err != nil {
			return err
		}
		q.wOnce = sync.Once{}
	}
	return nil
}
