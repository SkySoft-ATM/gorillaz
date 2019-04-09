# gorillaz: a Go common library

## Gorillaz offers:

### Centralized config

in configs/application.properties
can override with flags

### Easy streaming over gRPC

Producer: 
```go
g := gorillaz.New(nil)

p, err := g.NewStreamProvider("myNiceStream")
if err != nil {
    panic(err)
}

g.Run()

// Submit a value every second
for {
    event := &stream.Event{
        Value: []byte("something wonderful"),
    }
    p.Submit(event)
    time.Sleep(1 * time.Second)
}
    
```

Consumer:
```go
g := gorillaz.New(nil)
g.Run()

consumer, err := g.NewStreamConsumer("myNiceStream", gaz.IPEndpoint, strings.Split("localhost:8080", ","))
if err != nil {
    panic(err)
}


for evt := range consumer.EvtChan {
    fmt.Println(evt)
}
```

You will find more examples in the cmd folder

### A web server



### Prometheus integration

Running gorillaz starts a prometheus endpoint to collect metrics.



### Tracing

Tracing is done through zipkin, it can be configured with these properties:
```go
tracing.enabled=true
tracing.service.name=testProducer
tracing.collector.url=http://127.0.0.1:9411/api/v1/spans
```

### Logging

Gorillaz offers 2 loggers:

gorillaz.Log is a [zap.Logger](https://github.com/uber-go/zap)  that offers good performance but requires a bit more work when logging variables:

```go
gorillaz.Log.Error("Error while doing something", zap.Error(err))
```


gorillaz.Sugar is a [zap.SugaredLogger](https://github.com/uber-go/zap) that is slower but more convenient

```go
gorillaz.Sugar.Error("Error while doing something %v", err)
```