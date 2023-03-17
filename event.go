package events

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go-micro.dev/v4/logger"
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
	Name() string
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

type DefaultListenerConfig struct {
	// BufSize all unconsumed local event will present in memory channel, event message could be lost if machine shuting down emerging
	// make the handleEvent as small as possible to minimize the circlestance
	BufSize int
	// NextRetries if read next message failed, it should retry automatically NexRetries times
	NextRetryStrategy NextRetryStrategy
	Logger            logger.Logger
}

type DefaultListenerOption func(cfg *DefaultListenerConfig)

type NextRetryStrategy func(retry int, err error) bool

func BufSize(size int) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.BufSize = size
	}
}

func NextRetry(strategy NextRetryStrategy) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.NextRetryStrategy = strategy
	}
}

func Logger(logger logger.Logger) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.Logger = logger
	}
}

func RetryAny(shoudRetry int, waitUnit time.Duration) NextRetryStrategy {
	return func(retry int, err error) bool {
		time.Sleep(waitUnit * time.Duration(retry+1))
		return retry < shoudRetry
	}
}

func completeListenConfig(cfg *DefaultListenerConfig) {
	if cfg.BufSize <= 0 {
		cfg.BufSize = 1
	}
	if cfg.NextRetryStrategy == nil {
		cfg.NextRetryStrategy = RetryAny(3, time.Second)
	}
	if cfg.Logger == nil {
		cfg.Logger = logger.DefaultLogger
	}
}

func DefaultListener(opts ...DefaultListenerOption) EventListener {
	var cfg DefaultListenerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	completeListenConfig(&cfg)
	buf := make(chan *Event, cfg.BufSize)
	return Listen(func(ctx context.Context, q EventQueue, handler EventHandler) error {
		errCh := make(chan error)
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			for {
				select {
				case e := <-buf:
					if err := handler.HandleEvent(ctx, e); err != nil {
						if errors.Is(err, ListenComplete) {
							errCh <- nil
							close(errCh)
							cancel()
							return
						}
						cfg.Logger.Logf(logger.ErrorLevel, "handle event: %s", err.Error())
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
					retry := 0
					for {
						if err := q.Next(ctx, &e); err != nil {
							cfg.Logger.Logf(logger.ErrorLevel, "read next from queue[%s](retry %d): %s", q.Name(), retry, err.Error())
							if cfg.NextRetryStrategy(retry, err) {
								retry++
								continue
							}
							errCh <- err
							cancel()
							return
						}
						break
					}
					buf <- &e
					cfg.Logger.Logf(logger.InfoLevel, "read from queue [%s]: %v", q.Name(), e)
				}
			}
		}()
		return <-errCh
	})
}
