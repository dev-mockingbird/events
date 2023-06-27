package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	Next(ctx context.Context, e *Event, listenerId ...string) error
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

// Listener, the event queue listener, it will get the next event and pass it to handler
type Listener interface {
	// Listen, start listen event queue
	Listen(ctx context.Context, q EventBus, handle Handler) error
}

type Listen func(ctx context.Context, q EventBus, handle Handler) error

func (listen Listen) Listen(ctx context.Context, q EventBus, handle Handler) error {
	return listen(ctx, q, handle)
}

// DefaultListenerConfig config for default listener
type DefaultListenerConfig struct {
	// BufSize all unconsumed local event will present in memory channel, event message could be lost if machine shuting down emerging
	// make the handle as small as possible to minimize the circlestance
	BufSize int
	// NextRetries if read next message failed, it should retry automatically NexRetries times
	NextRetryStrategy NextRetryStrategy
	Id                string
	Logger            logf.Logfer
}

// DefaultListenerOption the argument type for DefaultListener
type DefaultListenerOption func(cfg *DefaultListenerConfig)

// NextRetryStrategy a callback for judging retry or not if grab next failed
type NextRetryStrategy func(retry int, err error) bool

// BufSize config the chan buffer size
func BufSize(size int) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.BufSize = size
	}
}

// NextRetry config the next retry strategy
func NextRetry(strategy NextRetryStrategy) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.NextRetryStrategy = strategy
	}
}

// Logger config logger
func Logger(logger logf.Logfer) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.Logger = logger
	}
}

// RetryAny a simple retry strategy
func RetryAny(shoudRetry int, waitUnit time.Duration) NextRetryStrategy {
	return func(retry int, err error) bool {
		time.Sleep(waitUnit * time.Duration(retry+1))
		return retry < shoudRetry
	}
}

func Id(id string) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.Id = id
	}
}

// completeListenConfig set default configuration for default listener
func completeListenConfig(cfg *DefaultListenerConfig) {
	if cfg.BufSize <= 0 {
		cfg.BufSize = 1
	}
	if cfg.NextRetryStrategy == nil {
		cfg.NextRetryStrategy = RetryAny(3, time.Second)
	}
	if cfg.Logger == nil {
		cfg.Logger = logf.New(logf.LogLevel(logf.Info))
	}
	if cfg.Id == "" {
		cfg.Id = uuid.NewString()
	}
}

// DefaultListener return a default listener
func DefaultListener(opts ...DefaultListenerOption) Listener {
	var cfg DefaultListenerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	completeListenConfig(&cfg)
	return Listen(func(ctx context.Context, q EventBus, handler Handler) error {
		if reg, ok := q.(ListenerRegisterer); ok {
			fmt.Printf("listen: %s\n", cfg.Id)
			reg.RegisterListener(cfg.Id)
			defer reg.UnregisterListener(cfg.Id)
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				done, err := func() (bool, error) {
					// e := GetEvent("")
					// defer Put(e)
					e := &Event{}
					retry := 0
					for {
						if err := q.Next(ctx, e, cfg.Id); err != nil {
							switch {
							case errors.Is(err, context.Canceled):
								cfg.Logger.Logf(logf.Warn, "context canceled listening")
								return true, nil
							case !errors.Is(err, io.EOF) && cfg.NextRetryStrategy(retry, err):
								cfg.Logger.Logf(logf.Error, "read next from queue[%s](retry %d): %s", q.Name(), retry, err.Error())
								retry++
								continue
							}
							cfg.Logger.Logf(logf.Error, "read next from event bus: %s", err.Error())
							return true, err
						}
						break
					}
					cfg.Logger.Logf(logf.Trace, "received message [%s: %s] queue [%s]", e.Type, e.ID, q.Name())
					cfg.Logger.Logf(logf.Trace, " payload: %s", e.Payload)
					if err := handler.Handle(ctx, e); err != nil {
						if errors.Is(err, ListenComplete) {
							cfg.Logger.Logf(logf.Info, "listen canceled by handler")
							return true, nil
						}
						cfg.Logger.Logf(logf.Error, "%s: %s", e.Type, err.Error)
					}
					return false, nil
				}()
				if done || err != nil {
					return err
				}
			}
		}
	})
}
