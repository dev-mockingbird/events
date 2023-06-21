package events

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

const kafkaTypeKey = "__type__"

type kafkabus struct {
	config KafkaEventBusConfig
	w      *kafka.Writer
	wOnce  sync.Once
	r      *kafka.Reader
	rOnce  sync.Once
}

type KafkaEventBusConfig struct {
	Brokers      []string
	Topic        string
	ConsumerName string
}

type KafkaEventBusOption func(config *KafkaEventBusConfig)

func KafkaBrokers(brokers ...string) KafkaEventBusOption {
	return func(config *KafkaEventBusConfig) {
		config.Brokers = brokers
	}
}

func KafkaTopic(topic string) KafkaEventBusOption {
	return func(config *KafkaEventBusConfig) {
		config.Topic = topic
	}
}

// Deprecated: KafkaConsumerName is deprecated, use KafkaConsumer instead
func KafkaConsumerName(name string) KafkaEventBusOption {
	return KafkaConsumer(name)
}

func KafkaConsumer(name string) KafkaEventBusOption {
	return func(config *KafkaEventBusConfig) {
		config.ConsumerName = name
	}
}

func KafkaBus(opts ...KafkaEventBusOption) EventBus {
	q := kafkabus{}
	for _, opt := range opts {
		opt(&q.config)
	}
	return &q
}

func (q *kafkabus) Name() string {
	return "kafka-" + q.config.Topic + "-" + q.config.ConsumerName
}

func (q *kafkabus) Add(ctx context.Context, e *Event) (err error) {
	q.wOnce.Do(func() {
		q.w = &kafka.Writer{
			Addr:  kafka.TCP(q.config.Brokers...),
			Topic: q.config.Topic,
		}
	})
	if err := e.PackPayload(); err != nil {
		return err
	}
	msg := kafka.Message{
		Key:     []byte(e.ID),
		Value:   e.Payload,
		Headers: make([]kafka.Header, len(e.Metadata)+1),
	}
	msg.Headers[0] = kafka.Header{Key: kafkaTypeKey, Value: []byte(e.Type)}
	var i int = 1
	for k, v := range e.Metadata {
		msg.Headers[i] = kafka.Header{Key: k, Value: []byte(v)}
	}
	if err := q.w.WriteMessages(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (q *kafkabus) Next(ctx context.Context, e *Event, listenerId ...string) (err error) {
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
		if h.Key == kafkaTypeKey {
			e.Type = string(val)
			continue
		}
		e.Metadata[h.Key] = string(val)
	}
	e.Payload = make([]byte, len(msg.Value))
	copy(e.Payload, msg.Value)
	key := make([]byte, len(msg.Key))
	copy(key, msg.Key)
	e.ID = string(key)
	if q.config.ConsumerName != "" {
		err = q.r.CommitMessages(ctx, msg)
	}
	return
}

func (q *kafkabus) Close() error {
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
