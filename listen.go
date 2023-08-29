package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dev-mockingbird/logf"
)

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
	// NextRetries if read next message failed, it should retry automatically NexRetries times
	NextRetryStrategy NextRetryStrategy
	Logger            logf.Logger
}

// DefaultListenerOption the argument type for DefaultListener
type DefaultListenerOption func(cfg *DefaultListenerConfig)

// NextRetry config the next retry strategy
func NextRetry(strategy NextRetryStrategy) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.NextRetryStrategy = strategy
	}
}

// Logger config logger
func Logger(logger logf.Logger) DefaultListenerOption {
	return func(cfg *DefaultListenerConfig) {
		cfg.Logger = logger
	}
}

// completeListenConfig set default configuration for default listener
func completeListenConfig(cfg *DefaultListenerConfig) {
	if cfg.NextRetryStrategy == nil {
		cfg.NextRetryStrategy = RetryAny(50, time.Second)
	}
	if cfg.Logger == nil {
		cfg.Logger = logf.New(logf.LogLevel(logf.Info))
	}
}

// DefaultListener return a default listener
func DefaultListener(name string, opts ...DefaultListenerOption) Listener {
	cfg := DefaultListenerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	completeListenConfig(&cfg)
	cfg.Logger = cfg.Logger.Prefix(fmt.Sprintf("event listener [%s]: ", name))
	return Listen(func(ctx context.Context, q EventBus, handler Handler) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				done, err := func() (bool, error) {
					e := GetEvent("")
					defer Put(e)
					retry := 0
					for {
						cfg.Logger.Logf(logf.Trace, "receiving next...")
						if err := q.Next(ctx, name, e); err != nil {
							switch {
							case errors.Is(err, context.Canceled):
								cfg.Logger.Logf(logf.Info, "listen canceled by handler (read next)")
								return true, nil
							case !errors.Is(err, io.EOF) && cfg.NextRetryStrategy(retry, err):
								cfg.Logger.Logf(logf.Info, "read next from event bus[%s](retry %d): %s. should retry again.", name, retry, err.Error())
								retry++
								continue
							}
							return true, err
						}
						break
					}
					cfg.Logger.Logf(logf.Debug, "received message [%s: %s]", e.Type, e.ID)
					cfg.Logger.Logf(logf.Trace, " payload: %s", e.Payload)
					if err := handler.Handle(ctx, e); err != nil {
						if errors.Is(err, ListenComplete) {
							cfg.Logger.Logf(logf.Info, "listen canceled by handler (handle event)")
							return true, nil
						}
						return false, err
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
