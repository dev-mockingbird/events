package events

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemoryEventBus_moreListener(t *testing.T) {
	q := MemoryEventBus("test", 10)
	ctx := context.Background()
	var wg sync.WaitGroup
	total := 0
	result := 0
	var lock sync.Mutex
	for i := 0; i < 10; i++ {
		total += i
		wg.Add(1)
		go func(id int) {
			DefaultListener(Id(fmt.Sprintf("%d", id))).Listen(ctx, q, Handle(func(ctx context.Context, e *Event) error {
				lock.Lock()
				result += id
				lock.Unlock()
				fmt.Printf("listener: %d. received: %s\n", id, e.Type)
				wg.Done()
				return ListenComplete
			}))
		}(i)
	}
	time.Sleep(time.Second)
	q.Add(ctx, New("test"))
	wg.Wait()
	if total != result {
		t.Fatal("error")
	}
}

func TestMemoryEventBus(t *testing.T) {
	q := MemoryEventBus("test", 10)
	q.(ListenerRegisterer).RegisterListener("1")
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
			if err = q.Next(context.Background(), &e, "1"); err != nil {
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
	reg := q.(ListenerRegisterer)
	reg.RegisterListener("1")
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error)
	go func() {
		var e Event
		if err := q.Next(ctx, &e, "1"); err != nil {
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
	var total int
	var lock sync.Mutex
	for i := 0; i < 100; i++ {
		q := MemoryEventBus("test", 10)
		req := q.(ListenerRegisterer)
		req.RegisterListener("1")
		wg.Add(1)
		go func(q EventBus) {
			defer wg.Done()
			var e Event
			if err := q.Next(ctx, &e, "1"); err != nil {
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
	q := MemoryEventBus("test", 10)
	if err := q.Add(ctx, New("test", Json(1))); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if total != 100 {
		t.Fatal("more consumer failed")
	}
}

func TestMemoryBus_Close(t *testing.T) {
	q := MemoryEventBus("test", 10)
	req := q.(ListenerRegisterer)
	req.RegisterListener("1")
	errCh := make(chan error)
	go func() {
		var e Event
		if err := q.Next(context.Background(), &e, "1"); err != nil {
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
