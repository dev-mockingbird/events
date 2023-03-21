package events

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dev-mockingbird/logf"
	"github.com/golang/mock/gomock"
)

func TestDefaultListener(t *testing.T) {
	rand.Seed(time.Now().UnixMicro())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	listener := DefaultListener(BufSize(10))
	q := MemoryEventBus("test", 10)
	var total int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			total += i
			q.Add(context.Background(), New("test", Json(i)))
		}
	}()
	var ct int
	go func() {
		defer wg.Done()
		listener.Listen(context.Background(), q, Handle(func(ctx context.Context, e *Event) error {
			var i int
			if err := e.UnpackPayload(&i); err != nil {
				return err
			}
			ct += i
			if ct >= total {
				return ListenComplete
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
	ch := make(chan error)
	go func() {
		err := DefaultListener().Listen(ctx, q, LogHandler(logf.New()))
		ch <- err
	}()
	time.Sleep(time.Second)
	cancel()
	err := <-ch
	if err != nil {
		t.Fatal(err)
	}
}
