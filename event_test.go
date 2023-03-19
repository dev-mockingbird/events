package events

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
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
			q.Add(context.Background(), NewEvent("test", []byte(fmt.Sprintf("%d", i))))
		}
	}()
	var ct int
	go func() {
		defer wg.Done()
		listener.Listen(context.Background(), q, HandleEvent(func(ctx context.Context, e *Event) error {
			i, _ := strconv.Atoi(string(e.Data))
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
