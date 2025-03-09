package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dev-mockingbird/logf"
)

const (
	// EncodingHing indicate the event.Data encoding
	Encoding = "encoding"
	// EncodingJson indicate the event.Data is json encoded
	EncodingJson = "json"
	// EncodingProto indicate the event.Data is proto encoded
	EncodingProto = "proto"
)

var (
	// ErrNoEncodingHint
	ErrNoEncodingHint   = errors.New("no encoding hint presence in metadata")
	UnsupportedEncoding = func(hint string) error {
		return fmt.Errorf("unsupported encoding hint: %s", hint)
	}
	LogHandler = func(lgr logf.Logfer) Handler {
		return Handle(func(e *Event) error {
			bs, err := json.Marshal(e)
			if err != nil {
				return err
			}
			lgr.Logf(logf.Info, "received event: %s", bs)
			return nil
		})
	}
	eventPool = sync.Pool{
		New: func() interface{} {
			return &Event{}
		},
	}
)

// Event the event representation definition
type Event struct {
	// Name event type, it should a dot joint string, such as "channel.message.created"
	Name string `json:"name"`
	// Metadata the metadata describes the primary info as a significant part of the event
	// such as {encoding-hint: encoding/json}, it expresses how to decode the payload
	// key should be "-" splited string and all lower cases
	// value should be "/" splited string
	Metadata map[string]string `json:"metadata,omitempty"`
	// CreatedAt
	CreateTimestamp int64 `json:"create_timestamp"`
	// Payload
	Payload []byte `json:"data,omitempty"`

	payloader Payloader `json:"-"`

	payloadPacked bool `json:"-"`
}

func Copy(dst, src *Event) {
	dst.Name = src.Name
	dst.Metadata = make(map[string]string, len(src.Metadata))
	for k, v := range src.Metadata {
		dst.Metadata[k] = v
	}
	dst.CreateTimestamp = src.CreateTimestamp
	if len(src.Payload) != 0 {
		dst.Payload = make([]byte, len(src.Payload))
		_ = copy(dst.Payload, src.Payload)
	}
	if src.payloader != nil {
		dst.payloader = src.payloader
	}
	dst.payloadPacked = src.payloadPacked
}

// New an event, it should use with With method to set the encoding-hint if payload emerged
// example:
//
//	events.New("test", []byte("{\"name\": \"\hello\"}")).With(events.EncodingHint, EncodingJson)
func New(name string, payloads ...Payloader) *Event {
	return &Event{
		Name:            name,
		Metadata:        make(map[string]string),
		CreateTimestamp: time.Now().Unix(),
		payloader: func() Payloader {
			if len(payloads) > 0 {
				return payloads[0]
			}
			return nil
		}(),
		payloadPacked: false,
	}
}

// Get evnet from event pool
func Get(name string, payloads ...Payloader) *Event {
	e := eventPool.Get().(*Event)
	e.Name = name
	e.Metadata = make(map[string]string)
	e.CreateTimestamp = time.Now().Unix()
	e.Payload = nil
	e.payloader = func() Payloader {
		if len(payloads) > 0 {
			return payloads[0]
		}
		return nil
	}()
	e.payloadPacked = false
	return e
}

// Put event to pool
func Put(e *Event) {
	eventPool.Put(e)
}

// With, set the metadata of an event
func (e *Event) With(k, v string) *Event {
	e.Metadata[k] = v
	return e
}

// PackPayload, set payload
func (e *Event) PackPayload() error {
	if e.payloader == nil || e.payloadPacked {
		return nil
	}
	var err error
	if e.Payload, err = e.payloader.Payload(); err != nil {
		return err
	}
	e.With(Encoding, e.payloader.Encoding())

	return nil
}

// UnpackPayload
func (e *Event) UnpackPayload(data any, unpackers ...PayloadUnpacker) error {
	if len(e.Payload) == 0 {
		return errors.New("payload empty")
	}
	hint, ok := e.Metadata[Encoding]
	if !ok {
		return ErrNoEncodingHint
	}
	for _, unpacker := range unpackers {
		if unpacker.Encoding() == hint {
			return unpacker.Unpack(e.Payload, data)
		}
	}
	for _, unpacker := range globalUnpackers {
		if unpacker.Encoding() == hint {
			return unpacker.Unpack(e.Payload, data)
		}
	}
	return UnsupportedEncoding(hint)
}

type Emitter interface {
	Emit(ctx context.Context, e *Event) error
}

type Listener interface {
	Listen(ctx context.Context, handler Handler) error
	Stop() error
}

// EventQueue, an event bus
type EventQueue interface {
	// Topic
	Topic() string
	// Push event to the queue
	Push(ctx context.Context, e *Event) error
	// Pop event from queue
	Pop(ctx context.Context, consumer string, e *Event) error
}

type EmittedEventQueue interface {
	Emitter() Emitter
	Listener(name string, opts ...Option) Listener
}

// Closer
type Closer interface {
	// Close
	Close() error
}

// Handler, event handler
type Handler interface {
	// Handle, handle the event. if the method returns an error, the listener should quit listen with the error.
	// ListenComplete indicates listener that the listen should be completed. if this special "error" returned, the listen should quit without error
	Handle(e *Event) error
}

// Handle is an sophisticated Handler which transforms a function to a handler
// example:
//
//	events.Handle(func(context.Background(), e *Event) error { return nil })
type Handle func(e *Event) error

// Handle implement the Handler
func (handle Handle) Handle(e *Event) error {
	return handle(e)
}
