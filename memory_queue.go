package events

import "context"

type memoryQueue struct {
	buffer chan Event
}

func MemoryQueue(bufSize int) EventQueue {
	return &memoryQueue{buffer: make(chan Event, bufSize)}
}

func (q memoryQueue) Add(ctx context.Context, e *Event) error {
	q.buffer <- *e
	return nil
}

func (q memoryQueue) Next(ctx context.Context, e *Event) error {
	*e = <-q.buffer
	return nil
}
