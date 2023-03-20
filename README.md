## events

An event system abstraction

```golang

q := events.KafkaBus()

logger := logf.New()

ctx := context.Background()

go func() {
    listener := events.DefaultListener(Logger(logger))
    listener.Listen(ctx, q, events.Handle(func(ctx context.Context, e *events.Event) error {
        var payload struct {
            Name string
        }
        if err := e.UnpackPayload(&payload); err != nil {
            return err
        }
        logger.Logf(logger.InfoLevel, "received event: %#v", e)
        logger.Logf(logger.InfoLevel, "received event payload: %#v", payload)
        return nil
    }))
}()

q.Add(ctx, events.New("test", Json(struct{Name string}{Name: "hello"})))

```
