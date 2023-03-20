package events

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestKafkaQueue(t *testing.T) {
	broker := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TEST_TOPIC")
	if broker == "" || topic == "" {
		t.Logf("no kafka broker found or no topic found! cancel test")
		return
	}
	brokers := strings.Split(broker, ",")
	q := KafkaQueue(KafkaBrokers(brokers...), KafkaTopic(topic), KafkaConsumerName("test"))
	ctx := context.Background()
	var total int
	for i := 0; i < 10; i++ {
		total += i
		if err := q.Add(ctx, New("test", Json(i))); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	rt := 0
	for {
		var e Event
		if err = q.Next(ctx, &e); err != nil {
			break
		}
		if e.ID == "" {
			err = errors.New("id empty")
			break
		}
		if e.Type == "" {
			err = errors.New("type empty")
			break
		}
		var i int
		if err = e.UnpackPayload(&i); err != nil {
			break
		}
		rt += i
		if rt >= total {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if rt != total {
		t.Fatal("failed")
	}
}
