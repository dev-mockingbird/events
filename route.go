package events

import (
	"context"
	"errors"
)

var (
	ErrUnsupportedEventType = errors.New("unsuppored event typee")
)

type Router interface {
	Handler
	On(typ string, h Handler) Router
}

type router struct {
	records map[string]Handler
}

var _ Router = &router{}

func On(typ string, h Handler) Router {
	r := router{records: make(map[string]Handler)}
	return r.On(typ, h)
}

func (r *router) On(typ string, h Handler) Router {
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
