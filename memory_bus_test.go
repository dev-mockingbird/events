package events

import (
	"context"
	"sync"
	"testing"
)

func TestMemoryQueue(t *testing.T) {
	q := MemoryEventBus(10)
	var wg sync.WaitGroup
	wg.Add(2)
	var total int
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			total += i
			q.Add(context.Background(), New("test", Json(i)))
		}
	}()
	var ct int
	var err error
	go func() {
		defer wg.Done()
		for {
			var e Event
			if err = q.Next(context.Background(), &e); err != nil {
				return
			}
			var i int
			if err = e.UnpackPayload(&i); err != nil {
				return
			}
			ct += i
			if ct >= total {
				break
			}
		}
	}()
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if ct != total {
		t.Fatal("not equal")
	}
}
