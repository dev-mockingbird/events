package events

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestMemoryQueue(t *testing.T) {
	q := MemoryQueue(10)
	var wg sync.WaitGroup
	wg.Add(2)
	var total int
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			total += i
			q.Add(context.Background(), NewEvent("test", []byte(fmt.Sprintf("%d", i))))
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
			i, _ := strconv.Atoi(string(e.Data))
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
