## events

An event system abstractation.

```golang

q := events.MemoryQueue(10)

logger := logf.New()

ctx := context.Background()

go func() {
    listener := events.DefaultListener(Logger(logger))
    listener.Listen(ctx, q, events.Handle(func(ctx context.Context, e *events.Event) error {
        logger.Logf(logger.InfoLevel, "received event: %#v", e)
        return nil
    }))
}()

q.Add(ctx, events.New("test", []))

```
