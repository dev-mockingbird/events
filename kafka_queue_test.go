package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	q := KafkaQueue(KafkaQueueConfig{
		Brokers:      brokers,
		Topic:        topic,
		ConsumerName: "test",
	})
	ctx := context.Background()
	var total int
	for i := 0; i < 10; i++ {
		total += i
		if err := q.Add(ctx, NewEvent("test", []byte(fmt.Sprintf("%d", i)))); err != nil {
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
		i, _ := strconv.Atoi(string(e.Data))
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
