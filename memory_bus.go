package events

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

var memoryEventBusChans map[string]*chan Event
var memoryEventBuses map[string]map[uint32]*memorybus
var memoryEventBusChanslock sync.RWMutex
var memoryEventBuseslock sync.RWMutex

func init() {
	memoryEventBusChans = make(map[string]*chan Event)
	memoryEventBuses = make(map[string]map[uint32]*memorybus)
}

type memorybus struct {
	id      uuid.UUID
	name    string
	local   *chan Event
	bufSize int
}

func initEventBus(m *memorybus) {
	initEventbusGlobalChan(m)
	initGlobalMemorybuses(m)
}

func reinitEventbusGlolalChan(m *memorybus) {
	if _, ok := memoryEventBusChans[m.name]; !ok {
		close(*memoryEventBusChans[m.name])
	}
	initEventbusGlobalChan(m)
}

func initEventbusGlobalChan(m *memorybus) {
	memoryEventBusChanslock.Lock()
	defer memoryEventBusChanslock.Unlock()
	if _, ok := memoryEventBusChans[m.name]; !ok {
		ch := make(chan Event, 100)
		memoryEventBusChans[m.name] = &ch
		go func(ch *chan Event) {
			for {
				e, ok := <-*ch
				if !ok {
					return
				}
				memoryEventBuseslock.Lock()
				for n, bus := range memoryEventBuses[m.name] {
					go func(ch *chan Event, e Event, n uint32) {
						*ch <- e
					}(bus.local, e, n)
				}
				memoryEventBuseslock.Unlock()
			}
		}(memoryEventBusChans[m.name])
	}
}

func initGlobalMemorybuses(m *memorybus) {
	memoryEventBuseslock.Lock()
	defer memoryEventBuseslock.Unlock()
	if _, ok := memoryEventBuses[m.name]; !ok {
		memoryEventBuses[m.name] = make(map[uint32]*memorybus)
	}
	if _, ok := memoryEventBuses[m.name][m.id.ID()]; !ok {
		memoryEventBuses[m.name][m.id.ID()] = m
	}
}

func MemoryEventBus(name string, bufSize int) EventBus {
	name = "memory-" + name
	ch := make(chan Event)
	b := memorybus{
		name:    name,
		bufSize: bufSize,
		local:   &ch,
		id:      uuid.New(),
	}
	initEventBus(&b)
	return &b
}

func (q *memorybus) Name() string {
	return q.id.String()
}

func (q *memorybus) Add(ctx context.Context, e *Event) error {
	if err := e.PackPayload(); err != nil {
		return err
	}
	ch := make(chan struct{})
	go func() {
		memoryEventBusChanslock.RLock()
		*memoryEventBusChans[q.name] <- *e
		memoryEventBusChanslock.RUnlock()
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
				reinitEventbusGlolalChan(q)
				close(*q.local)
				ch := make(chan Event)
				q.local = &ch
				err = ctx.Err()
			}
		}
	}()
	<-ch
	done <- struct{}{}
	return err
}

func (q *memorybus) Next(ctx context.Context, e *Event) error {
	ch := make(chan struct{})
	go func() {
		*e = <-*q.local
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
				reinitEventbusGlolalChan(q)
				close(*q.local)
				ch := make(chan Event)
				q.local = &ch
				err = ctx.Err()
			}
		}
	}()
	<-ch
	done <- struct{}{}
	return err
}

func (q *memorybus) Close() error {
	memoryEventBuseslock.Lock()
	if buses, ok := memoryEventBuses[q.name]; ok {
		delete(buses, q.id.ID())
		close(*q.local)
	}
	hasbuses := len(memoryEventBuses[q.name]) > 0
	memoryEventBuseslock.Unlock()
	if !hasbuses {
		return nil
	}
	memoryEventBusChanslock.Lock()
	close(*memoryEventBusChans[q.name])
	delete(memoryEventBusChans, q.name)
	memoryEventBusChanslock.Unlock()
	return nil
}
