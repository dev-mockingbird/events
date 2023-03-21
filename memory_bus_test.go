package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryEventBus(t *testing.T) {
	q := MemoryEventBus("test", 10)
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

func TestMemoryBusCancel(t *testing.T) {
	q := MemoryEventBus("test", 10)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error)
	go func() {
		var e Event
		if err := q.Next(ctx, &e); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	time.Sleep(time.Second)
	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
