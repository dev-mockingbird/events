package events

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKafkaBus(t *testing.T) {
	// broker := "127.0.0.1:9092"
	// topic := "test-1234"
	broker := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TEST_TOPIC")
	if broker == "" || topic == "" {
		t.Logf("no kafka broker found or no topic found! cancel test")
		return
	}
	brokers := strings.Split(broker, ",")
	ctx := context.Background()
	var err error
	result := make(map[int]int)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			q := KafkaBus(KafkaBrokers(brokers...), KafkaTopic(topic), KafkaConsumer(fmt.Sprintf("%d", id)))
			DefaultListener().Listen(ctx, q, Handle(func(ctx context.Context, e *Event) error {
				var i int
				if err = e.UnpackPayload(&i); err != nil {
					panic(err)
				}
				lock.Lock()
				v := i
				if vl, ok := result[id]; ok {
					v += vl
				}
				result[id] = v
				lock.Unlock()
				t.Logf("id: %d, value: %d\n", id, result[id])
				if i >= 9 {
					return ListenComplete
				}
				return nil
			}))
		}(i)
	}
	time.Sleep(time.Second)
	q := KafkaBus(KafkaBrokers(brokers...), KafkaTopic(topic), KafkaConsumer("test"))
	var total int
	for i := 0; i < 10; i++ {
		total += i
		if err := q.Add(ctx, New("test", Json(i))); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	time.Sleep(time.Second)
	for id, v := range result {
		t.Logf("id: %d, v: %d, total: %d\n", id, v, total)
		if v != total {
			t.Fatal("not equal")
		}
	}
}
