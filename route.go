package events

import (
	"context"
	"errors"
)

var (
	ErrUnsupportedEventType = errors.New("unsuppored event typee")
)

type router struct {
	records map[string]Handler
}

func On(typ string, h Handler) *router {
	r := router{records: make(map[string]Handler)}
	return r.On(typ, h)
}

func (r *router) On(typ string, h Handler) *router {
	r.records[typ] = h
	return r
}

func (r *router) Handle(ctx context.Context, e *Event) error {
	handler, ok := r.records[e.Type]
	if !ok {
		return ErrUnsupportedEventType
	}
	return handler.Handle(ctx, e)
}
