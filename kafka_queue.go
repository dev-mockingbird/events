package events

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
)

const kafkaNameKey = "__name__"

type kafkaReadContext struct {
	reader  *kafka.Reader
	reading bool
}

type kafkaQueue struct {
	config      KafkaEventQueueConfig
	w           *kafka.Writer
	wOnce       sync.Once
	readers     map[string]*kafkaReadContext
	readersLock sync.RWMutex
}

type KafkaEventQueueConfig struct {
	Brokers []string
	Topic   string
}

type KafkaEventQueueOption func(config *KafkaEventQueueConfig)

func KafkaBrokers(brokers ...string) KafkaEventQueueOption {
	return func(config *KafkaEventQueueConfig) {
		config.Brokers = brokers
	}
}

func KafkaTopic(topic string) KafkaEventQueueOption {
	return func(config *KafkaEventQueueConfig) {
		config.Topic = topic
	}
}

func KafkaQueue(opts ...KafkaEventQueueOption) EventQueue {
	q := kafkaQueue{readers: make(map[string]*kafkaReadContext)}
	for _, opt := range opts {
		opt(&q.config)
	}
	return &q
}

func (q *kafkaQueue) Topic() string {
	return q.config.Topic
}

func (q *kafkaQueue) Push(ctx context.Context, e *Event) (err error) {
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
	msg.Headers[0] = kafka.Header{Key: kafkaNameKey, Value: []byte(e.Name)}
	var i int = 1
	for k, v := range e.Metadata {
		msg.Headers[i] = kafka.Header{Key: k, Value: []byte(v)}
	}
	if err := q.w.WriteMessages(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (q *kafkaQueue) Pop(ctx context.Context, consumer string, e *Event) (err error) {
	q.readersLock.RLock()
	readCtx, ok := q.readers[consumer]
	q.readersLock.RUnlock()
	if !ok {
		readCtx = &kafkaReadContext{
			reader: kafka.NewReader(kafka.ReaderConfig{
				Brokers: q.config.Brokers,
				Topic:   q.config.Topic,
				GroupID: consumer,
			}),
		}
		q.readersLock.Lock()
		q.readers[consumer] = readCtx
		q.readersLock.Unlock()
	}
	var msg kafka.Message
	readCtx.reading = true
	if msg, err = readCtx.reader.ReadMessage(ctx); err != nil {
		if !readCtx.reading && errors.Is(err, io.EOF) {
			err = nil
		}
		readCtx.reading = false
		return
	}
	readCtx.reading = false
	e.Metadata = make(map[string]string)
	for _, h := range msg.Headers {
		val := make([]byte, len(h.Value))
		copy(val, h.Value)
		if h.Key == kafkaNameKey {
			e.Name = string(val)
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

func (q *kafkaQueue) Close() error {
	q.readersLock.Lock()
	for _, reader := range q.readers {
		reader.reading = false
		if err := reader.reader.Close(); err != nil {
			return err
		}
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
