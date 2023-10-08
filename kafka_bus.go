package events

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

const kafkaTypeKey = "__type__"

type kafkabus struct {
	config      KafkaEventBusConfig
	w           *kafka.Writer
	wOnce       sync.Once
	readers     map[string]*kafka.Reader
	readersLock sync.RWMutex
}

type KafkaEventBusConfig struct {
	Brokers []string
	Topic   string
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

func KafkaBus(opts ...KafkaEventBusOption) EventBus {
	q := kafkabus{readers: make(map[string]*kafka.Reader)}
	for _, opt := range opts {
		opt(&q.config)
	}
	return &q
}

func (q *kafkabus) Name() string {
	return "kafka-" + q.config.Topic
}

func (q *kafkabus) Add(ctx context.Context, e *Event) (err error) {
	q.wOnce.Do(func() {
		q.w = &kafka.Writer{
			Addr:                   kafka.TCP(q.config.Brokers...),
			Topic:                  q.config.Topic,
			AllowAutoTopicCreation: true,
		}
	})
	if err := e.PackPayload(); err != nil {
		return err
	}
	msg := kafka.Message{
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

func (q *kafkabus) Next(ctx context.Context, listenerId string, e *Event) (err error) {
	q.readersLock.RLock()
	reader, ok := q.readers[listenerId]
	q.readersLock.RUnlock()
	if !ok {
		reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers: q.config.Brokers,
			Topic:   q.config.Topic,
			GroupID: listenerId,
		})
		q.readersLock.Lock()
		q.readers[listenerId] = reader
		q.readersLock.Unlock()
	}
	var msg kafka.Message
	if msg, err = reader.ReadMessage(ctx); err != nil {
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
	return
}

func (q *kafkabus) Close() error {
	q.readersLock.Lock()
	for k, reader := range q.readers {
		reader.Close()
		delete(q.readers, k)
	}
	q.readersLock.Unlock()
	if q.w != nil {
		if err := q.w.Close(); err != nil {
			return err
		}
		q.wOnce = sync.Once{}
	}
	return nil
}
