package events

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	ListenComplete = errors.New("listen complete")
)

type Event struct {
	ID   string
	Type string
	Data []byte
}

func NewEvent(typ string, data []byte) *Event {
	return &Event{ID: uuid.New().String(), Type: typ, Data: data}
}

type EventQueue interface {
	Add(ctx context.Context, e *Event) error
	Next(ctx context.Context, e *Event) error
}

type EventHandler interface {
	HandleEvent(ctx context.Context, e *Event) error
}

type HandleEvent func(ctx context.Context, e *Event) error

func (handle HandleEvent) HandleEvent(ctx context.Context, e *Event) error {
	return handle(ctx, e)
}

type EventListener interface {
	Listen(ctx context.Context, q EventQueue, handle EventHandler) error
}

type Listen func(ctx context.Context, q EventQueue, handle EventHandler) error

func (listen Listen) Listen(ctx context.Context, q EventQueue, handle EventHandler) error {
	return listen(ctx, q, handle)
}

func DefaultListener(bufSize int) EventListener {
	buf := make(chan *Event, bufSize)
	return Listen(func(ctx context.Context, q EventQueue, handler EventHandler) error {
		errCh := make(chan error)
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			for {
				select {
				case e := <-buf:
					if err := handler.HandleEvent(ctx, e); err != nil {
						errCh <- err
						cancel()
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					var e Event
					if err := q.Next(ctx, &e); err != nil {
						errCh <- err
						cancel()
						return
					}
					buf <- &e
				}
			}
		}()
		return <-errCh
	})
}
