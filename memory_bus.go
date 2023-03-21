package events

import (
	"context"
	"sync"
)

var memoryEventBusChans map[string]*chan Event
var memoryEventBusChanslock sync.RWMutex

func init() {
	memoryEventBusChans = make(map[string]*chan Event)
}

type memorybus struct {
	name    string
	bufSize int
}

func initChan(m memorybus) {
	memoryEventBusChanslock.Lock()
	defer memoryEventBusChanslock.Unlock()
	if ch, ok := memoryEventBusChans[m.name]; ok {
		close(*ch)
	}
	ch := make(chan Event, m.bufSize)
	memoryEventBusChans[m.name] = &ch
}

func MemoryEventBus(name string, bufSize int) EventBus {
	name = "memory-" + name
	b := memorybus{name: name, bufSize: bufSize}
	initChan(b)
	return &b
}

func (q memorybus) Name() string {
	return q.name
}

func (q memorybus) Add(ctx context.Context, e *Event) error {
	if err := e.PackPayload(); err != nil {
		return err
	}
	ch := make(chan struct{})
	go func() {
		*memoryEventBusChans[q.name] <- *e
		ch <- struct{}{}
	}()
	done := make(chan struct{})
	var err error
	go func() {
		for {
			select {
			case <-done:
				close(done)
				return
			case <-ctx.Done():
				initChan(q)
				err = ctx.Err()
			}
		}
	}()
	<-ch
	done <- struct{}{}
	return err
}

func (q memorybus) Next(ctx context.Context, e *Event) error {
	ch := make(chan struct{})
	go func() {
		*e = <-*memoryEventBusChans[q.name]
		ch <- struct{}{}
	}()
	done := make(chan struct{})
	var err error
	go func() {
		for {
			select {
			case <-done:
				close(done)
				return
			case <-ctx.Done():
				initChan(q)
				err = ctx.Err()
			}
		}
	}()
	<-ch
	done <- struct{}{}
	return err
}
