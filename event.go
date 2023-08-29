package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dev-mockingbird/logf"
	"github.com/google/uuid"
)

const (
	// EncodingHing indicate the event.Data encoding
	EncodingHint = "encoding-hint"
	// EncodingJson indicate the event.Data is json encoded
	EncodingJson = "encoding/json"
	// EncodingProto indicate the event.Data is proto encoded
	EncodingProto = "encoding/proto"
)

var (
	// ListenComplete the listener shold quit listen with no error if ListenComplete returned by event.Handler
	ListenComplete      = errors.New("listen complete")
	NoEncodingHint      = errors.New("no encoding hint presence in metadata")
	UnsupportedEncoding = func(hint string) error {
		return fmt.Errorf("unsupported encoding hint: %s", hint)
	}
	LogHandler = func(lgr logf.Logfer) Handler {
		return Handle(func(ctx context.Context, e *Event) error {
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

const (
	// StopListen the listener should quit listen with no error if StopListen type message received from queue
	StopListen = "listen.stop"
)

// Event the event representation definition
type Event struct {
	// ID event id, always a uuid, it will be auto generated when constructed by New
	ID string `json:"id"`
	// Type event type, it should a dot joint string, such as "channel.message.created"
	Type string `json:"type"`
	// Metadata the metadata describes the primary info as a significant part of the event
	// such as {encoding-hint: encoding/json}, it expresses how to decode the payload
	// key should be "-" splited string and all lower cases
	// value should be "/" splited string
	Metadata map[string]string `json:"metadata,omitempty"`
	// CreatedAt
	CreatedAt time.Time `json:"created_at"`
	// Payload
	Payload []byte `json:"data,omitempty"`

	payloader Payloader `json:"-"`

	payloadPacked bool `json:"-"`
}

func Copy(dst, src *Event) {
	dst.ID = src.ID
	dst.Type = src.Type
	dst.Metadata = make(map[string]string, len(src.Metadata))
	for k, v := range src.Metadata {
		dst.Metadata[k] = v
	}
	dst.CreatedAt = src.CreatedAt
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
func New(typ string, payloads ...Payloader) *Event {
	return &Event{
		ID:        uuid.New().String(),
		Type:      typ,
		Metadata:  make(map[string]string),
		CreatedAt: time.Now(),
		payloader: func() Payloader {
			if len(payloads) > 0 {
				return payloads[0]
			}
			return nil
		}(),
		payloadPacked: false,
	}
}

func GetEvent(typ string, payloads ...Payloader) *Event {
	e := eventPool.Get().(*Event)
	e.ID = uuid.New().String()
	e.Type = typ
	e.Metadata = make(map[string]string)
	e.CreatedAt = time.Now()
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
	e.With(EncodingHint, e.payloader.Encoding())

	return nil
}

// UnpackPayload
func (e *Event) UnpackPayload(data any, unpackers ...PayloadUnpacker) error {
	if len(e.Payload) == 0 {
		return errors.New("payload empty")
	}
	hint, ok := e.Metadata[EncodingHint]
	if !ok {
		return NoEncodingHint
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

// EventBus, an event bus
type EventBus interface {
	// Name, the queue must have a name, the logger will use this to record the activitity
	Name() string
	// Add, push event to the queue
	Add(ctx context.Context, e *Event) error
	// Next, grab next event from queue
	Next(ctx context.Context, listenerId string, e *Event) error
}

type ListenerRegisterer interface {
	RegisterListener(id string)
	UnregisterListener(id string)
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
	Handle(ctx context.Context, e *Event) error
}

// Handle, an sophisticated Handler which transforms a function to a handler
// example:
//
//	events.Handle(func(context.Background(), e *Event) error { return nil })
type Handle func(ctx context.Context, e *Event) error

// Handle implement the Handler
func (handle Handle) Handle(ctx context.Context, e *Event) error {
	return handle(ctx, e)
}
