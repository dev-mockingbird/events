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

func TestMemoryBus_MoreConsumer(t *testing.T) {
	ctx := context.Background()
	errCh := make(chan error, 10)
	var wg sync.WaitGroup
	wg.Add(10)
	var total int
	var lock sync.Mutex
	for i := 0; i < 10; i++ {
		q := MemoryEventBus("test", 10)
		go func(q EventBus) {
			defer wg.Done()
			var e Event
			if err := q.Next(ctx, &e); err != nil {
				errCh <- err
				return
			}
			var i int
			if err := e.UnpackPayload(&i); err != nil {
				t.Logf("err: %s", err.Error())
				return
			}
			lock.Lock()
			total += i
			lock.Unlock()
		}(q)
	}
	time.Sleep(time.Second)
	q := MemoryEventBus("test", 10)
	if err := q.Add(ctx, New("test", Json(1))); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if total != 10 {
		t.Fatal("more consumer failed")
	}
}

func TestMemoryBus_Close(t *testing.T) {
	q := MemoryEventBus("test", 10)
	errCh := make(chan error)
	go func() {
		var e Event
		if err := q.Next(context.Background(), &e); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	if err := q.(Closer).Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}
