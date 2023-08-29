package events

import "time"

// NextRetryStrategy a callback for judging retry or not if grab next failed
type NextRetryStrategy func(retry int, err error) bool

// RetryAny a simple retry strategy
func RetryAny(shoudRetry int, waitUnit time.Duration) NextRetryStrategy {
	return func(retry int, err error) bool {
		time.Sleep(waitUnit * time.Duration(retry+1))
		return retry < shoudRetry
	}
}
