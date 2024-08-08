## events

An event system abstraction

```golang

q := events.KafkaQueue(KafkaTopic("hello"))

logger := logf.New()

ctx := context.Background()

done := make(chan struct{})

go func() {
    listener := events.GetListener("hello-world", q, Logger(logger))
    listener.Listen(ctx, events.Handle(func(ctx context.Context, e *events.Event) error {
        var payload struct {
            Name string
        }
        if err := e.UnpackPayload(&payload); err != nil {
            return err
        }
        logger.Logf(logger.InfoLevel, "received event: %#v", e)
        logger.Logf(logger.InfoLevel, "received event payload: %#v", payload)
        listener.Stop()
        done <- struct{}{}
        return nil
    }))
}()

q.Add(ctx, events.New("test", Json(struct{Name string}{Name: "hello"})))

<- done
```


```golang

q := events.KafkaQueue(KafkaTopic("hello"))

el := events.EmitListener{Q: q, Name: "hello-world"}

done := make(chan struct{})

el.Listener().Listen(ctx, events.Handler(func(ctx contxt.Context, q *events.Event) error {
    done <- struct{}{}
    return nil
}))


el.Emitter().Emit(context.Background(), events.Get("hello-world", Json(struct{Name string}{Name: "hello"})))

<- done

```
