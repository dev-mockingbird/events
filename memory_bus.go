package events

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

var memorybuses *memorybusManager

func init() {
	memorybuses = &memorybusManager{memorybuses: make(map[string]*memorybus)}
}

type memorybusManager struct {
	memorybuses map[string]*memorybus
	lock        sync.RWMutex
}

func (m *memorybusManager) add(bus *memorybus) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.memorybuses[bus.name] = bus
}

func (m *memorybusManager) del(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	bus, ok := m.memorybuses[name]
	if !ok {
		return
	}
	for _, entry := range bus.entries {
		entry.lock.Lock()
		for _, listener := range entry.listeners {
			close(*listener)
		}
		entry.lock.Unlock()
	}
	bus.stoppedlock.Lock()
	bus.stopped = true
	bus.stoppedlock.Unlock()
	close(*bus.ch)
	delete(m.memorybuses, name)
}

func (m *memorybusManager) get(name string) *memorybus {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.memorybuses[name]
}

type memorybus struct {
	ch          *chan Event
	name        string
	entries     []*memorybusEntry
	stopped     bool
	stoppedlock sync.RWMutex
	lock        sync.Mutex
}

type memorybusEntry struct {
	id        uuid.UUID
	listeners map[string]*chan Event
	lock      sync.RWMutex
	memorybus *memorybus
}

func (m *memorybus) start() {
	go func() {
		for {
			e := <-*m.ch
			m.stoppedlock.Lock()
			stopped := m.stopped
			m.stoppedlock.Unlock()
			if stopped {
				return
			}
			for _, entry := range m.entries {
				go func(entry *memorybusEntry) {
					entry.lock.RLock()
					defer entry.lock.RUnlock()
					for _, listener := range entry.listeners {
						*listener <- e
					}
				}(entry)
			}
		}
	}()
}

func newMemorybusEntry(bus *memorybus, bufSize int) *memorybusEntry {
	return &memorybusEntry{
		id:        uuid.New(),
		listeners: make(map[string]*chan Event),
		memorybus: bus,
	}
}

var lock sync.Mutex

func MemoryEventBus(name string, bufSize int) EventBus {
	lock.Lock()
	defer lock.Unlock()
	name = "memory-" + name
	bus := memorybuses.get(name)
	if bus != nil {
		entry := newMemorybusEntry(bus, bufSize)
		bus.entries = append(bus.entries, entry)
		return entry
	}
	ch := make(chan Event, bufSize)
	bus = &memorybus{
		name: name,
		ch:   &ch,
	}
	entry := newMemorybusEntry(bus, bufSize)
	bus.entries = append(bus.entries, entry)
	memorybuses.add(bus)
	bus.start()
	return entry
}

func (q *memorybusEntry) Name() string {
	return q.memorybus.name + "-" + q.id.String()
}

func (q *memorybusEntry) Add(ctx context.Context, e *Event) error {
	if err := e.PackPayload(); err != nil {
		return err
	}
	ch := make(chan struct{}, 1)
	go func() {
		*q.memorybus.ch <- *e
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *memorybusEntry) RegisterListener(id string) {
	q.lock.Lock()
	ch := make(chan Event)
	q.listeners[id] = &ch
	q.lock.Unlock()
}

func (q *memorybusEntry) UnregisterListener(id string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if ch, ok := q.listeners[id]; ok {
		close(*ch)
	}
	delete(q.listeners, id)
}

func (q *memorybusEntry) Next(ctx context.Context, e *Event, listenerId ...string) error {
	ch := make(chan struct{}, 1)
	var err error
	go func() {
		if len(listenerId) == 0 {
			err = errors.New("no listener id found")
		}
		q.lock.RLock()
		lch, ok := q.listeners[listenerId[0]]
		q.lock.RUnlock()
		if !ok {
			err = errors.New("no listener found")
		}
		*e = <-*lch
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *memorybusEntry) Close() error {
	q.memorybus.lock.Lock()
	defer q.memorybus.lock.Unlock()
	for i := 0; i < len(q.memorybus.entries); i++ {
		if q == q.memorybus.entries[i] {
			q.lock.Lock()
			for _, listener := range q.listeners {
				close(*listener)
			}
			q.lock.Unlock()
			q.memorybus.entries = append(q.memorybus.entries[:i], q.memorybus.entries[i+1:]...)
		}
	}
	if len(q.memorybus.entries) == 0 {
		memorybuses.del(q.memorybus.name)
	}
	return nil
}
