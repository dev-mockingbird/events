package events

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
)

func TestDefaultListener(t *testing.T) {
	rand.Seed(time.Now().UnixMicro())
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	listener := DefaultListener(BufSize(10))
	q := MemoryQueue(10)
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
