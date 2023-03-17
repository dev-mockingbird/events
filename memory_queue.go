package events

import (
	"context"

	"github.com/google/uuid"
)

type memoryQueue struct {
	buffer chan Event
	name   string
}

func MemoryQueue(bufSize int) EventQueue {
	return &memoryQueue{buffer: make(chan Event, bufSize), name: uuid.NewString()}
}

func (q memoryQueue) Name() string {
	return "memory_" + q.name
}

func (q memoryQueue) Add(ctx context.Context, e *Event) error {
	q.buffer <- *e
	return nil
}

func (q memoryQueue) Next(ctx context.Context, e *Event) error {
	*e = <-q.buffer
	return nil
}
