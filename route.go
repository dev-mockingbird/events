package events

import (
	"fmt"
)

var (
	ErrUnsupportedEvent = func(name string) error {
		return fmt.Errorf("unsuppored event [%s]", name)
	}
)

type Router interface {
	Handler
	ON(name string, h Handler) Router
}

type router struct {
	records map[string]Handler
}

var _ Router = &router{}

func ON(name string, h Handler) Router {
	r := router{records: make(map[string]Handler)}
	return r.ON(name, h)
}

func (r *router) ON(typ string, h Handler) Router {
	r.records[typ] = h
	return r
}

func (r *router) Handle(e *Event) error {
	handler, ok := r.records[e.Name]
	if !ok {
		return ErrUnsupportedEvent(e.Name)
	}
	return handler.Handle(e)
}
