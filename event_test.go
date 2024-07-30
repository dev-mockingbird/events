package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dev-mockingbird/logf"
	"github.com/golang/mock/gomock"
)

func TestDefaultListener(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	q := MemoryEventBus("test", 10)
	listener := GetListener("test", q)
	var total int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			total += i
			q.Push(context.Background(), New("test", Json(i)))
		}
	}()
	var ct int
	go func() {
		defer wg.Done()
		listener.Listen(context.Background(), Handle(func(ctx context.Context, e *Event) error {
			var i int
			if err := e.UnpackPayload(&i); err != nil {
				return err
			}
			ct += i
			if ct >= total {
				listener.Stop()
			}
			return nil
		}))
	}()
	wg.Wait()
	if ct != total {
		t.Fatal("listen failed")
	}
}

func TestDefaultListenerCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := MemoryEventBus("test", 10)
	ch := make(chan error, 1)
	go func() {
		err := GetListener("test", q).Listen(ctx, LogHandler(logf.New()))
		ch <- err
	}()
	cancel()
	time.Sleep(time.Second)
	err := <-ch
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
