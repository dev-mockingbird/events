package events

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type memoryQueue struct {
	buffer chan Event
	name   string
}

func MemoryQueue(bufSize int) Queue {
	return &memoryQueue{buffer: make(chan Event, bufSize), name: fmt.Sprintf("%d", uuid.New().ID())}
}

func (q memoryQueue) Name() string {
	return "memory-" + q.name
}

func (q memoryQueue) Add(ctx context.Context, e *Event) error {
	if err := e.PackPayload(); err != nil {
		return err
	}
	q.buffer <- *e
	return nil
}

func (q memoryQueue) Next(ctx context.Context, e *Event) error {
	*e = <-q.buffer
	return nil
}
