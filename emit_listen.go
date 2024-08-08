package events

import (
	"context"
	"sync"
)

type EmitListener struct {
	Q             EventQueue
	Name          string
	ListenOptions []Option

	listener     Listener
	initListener sync.Once
}

func (eel *EmitListener) Emitter() Emitter {
	return eel
}

func (eel *EmitListener) Listener() Listener {
	return eel
}

func (el *EmitListener) Emit(ctx context.Context, e *Event) error {
	return el.Q.Push(ctx, e)
}

func (el *EmitListener) Listen(ctx context.Context, handler Handler) error {
	el.doInitListener()
	return el.listener.Listen(ctx, handler)
}

func (el *EmitListener) Stop() error {
	el.doInitListener()
	return el.listener.Stop()
}

func (el *EmitListener) doInitListener() {
	el.initListener.Do(func() {
		el.listener = GetListener(el.Name, el.Q, el.ListenOptions...)
	})
}
