package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/dev-mockingbird/logf"
)

type defaultListener struct {
	q             EventQueue
	name          string
	retryStrategy NextRetryStrategy
	logger        logf.Logger
	cancel        func()
	listening     bool
}

type Option func(l *defaultListener)

// NextRetry config the next retry strategy
func NextRetry(strategy NextRetryStrategy) Option {
	return func(cfg *defaultListener) {
		cfg.retryStrategy = strategy
	}
}

// Logger config logger
func Logger(logger logf.Logger) Option {
	return func(cfg *defaultListener) {
		cfg.logger = logger
	}
}

func (l *defaultListener) completeListenConfig() {
	if l.retryStrategy == nil {
		l.retryStrategy = RetryAny(50, time.Second)
	}
	if l.logger == nil {
		l.logger = logf.New(logf.LogLevel(logf.Info))
	}
}

func (l *defaultListener) Stop() error {
	if !l.listening {
		return nil
	}
	ch := make(chan struct{}, 1)
	go func() {
		l.listening = false
		l.cancel()
		ch <- struct{}{}
	}()
	<-ch
	return nil
}

func GetListener(name string, q EventQueue, opts ...Option) Listener {
	l := defaultListener{
		name: name,
		q:    q,
	}
	for _, apply := range opts {
		apply(&l)
	}
	l.completeListenConfig()
	return &l
}

func (l *defaultListener) Listen(ctx context.Context, handler Handler) error {
	defer func() {
		l.listening = false
		if err := recover(); err != nil {
			l.logger.Logf(logf.Fatal, "event listener[%s-%s]: panic: %v", l.name, l.q.Topic(), err)
			err = l.Listen(ctx, handler)
		}
	}()
	if l.listening {
		return fmt.Errorf("listener [%s-%s] is listening", l.name, l.q.Topic())
	}
	ctx, l.cancel = context.WithCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := l.listen(ctx, handler); err != nil {
				return err
			}
		}
	}
}

func (l *defaultListener) listen(ctx context.Context, handler Handler) error {
	l.listening = true
	e := Get("")
	defer Put(e)
	retry := 0
	for {
		if err := l.q.Pop(ctx, l.name, e); err != nil {
			switch {
			case errors.Is(err, context.Canceled):
				l.logger.Logf(logf.Info, "listen canceled by handler")
				return nil
			case !errors.Is(err, io.EOF) && l.retryStrategy(retry, err):
				l.logger.Logf(logf.Info, "read next from event topic[%s]: %s. should retry again.", l.name, retry, err.Error())
				retry++
				continue
			}
			l.logger.Logf(logf.Error, "read next from event topic[%s]: %s", err.Error())
			return err
		}
		break
	}
	l.logger.Logf(logf.Debug, "received message [%s]", e.Name)
	l.logger.Logf(logf.Trace, " payload: %s", e.Payload)
	if err := handler.Handle(e); err != nil {
		l.logger.Logf(logf.Error, "handler return an error: %s", err.Error())
	}
	return nil
}
