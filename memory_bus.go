package events

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type memorybus struct {
	buffer chan Event
	name   string
}

func MemoryEventBus(bufSize int) EventBus {
	return &memorybus{buffer: make(chan Event, bufSize), name: fmt.Sprintf("%d", uuid.New().ID())}
}

func (q memorybus) Name() string {
	return "memory-" + q.name
}

func (q memorybus) Add(ctx context.Context, e *Event) error {
	if err := e.PackPayload(); err != nil {
		return err
	}
	q.buffer <- *e
	return nil
}

func (q memorybus) Next(ctx context.Context, e *Event) error {
	*e = <-q.buffer
	return nil
}
