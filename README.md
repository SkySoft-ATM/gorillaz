# gorillaz: a Go common library

[![Build Status](https://travis-ci.com/SkySoft-ATM/gorillaz.svg?branch=master)](https://travis-ci.com/SkySoft-ATM/gorillaz)

## Gorillaz offers:

### Opinionated config

Setup your project config in configs/application.properties
You can override values by passing flags on the command line.

A service name and an environment must be provided in the configuration
```
service.name=testProducer
env=uat
```

### A web server
A common we server is started for metrics and healthchecks.
You can configure its port with this property:
```
http.port=12347
```

### Health check endpoints

2 health check endpoints that can be used for example in Kubernetes are provided:

/live
/ready

You can control them in the application with:
```go
gaz.SetLive(true)
gaz.SetReady(true)
```

They can be enabled or disabled with this property:
```
healtheck.enabled=false
```

### Prometheus integration

Running gorillaz starts a prometheus endpoint to collect metrics:
/metrics

You can disable this endpoint with this property:
```
prometheus.enabled=false
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

The log level is configured through this property:
```
log.level=info
```



### Easy streaming over gRPC

Producer: 
```go
g := gorillaz.New()

p, err := g.NewStreamProvider("myNiceStream", "my stream data type")
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
g := gorillaz.New()
consumer, err := g.ConsumeStream([]string{"localhost:9090"}, "myStreamName")
if err != nil {
    panic(err)
}

for evt := range consumer.EvtChan() {
    fmt.Println(evt)
}
```

Consumer with service discovery:
```go
g := gorillaz.New()
consumer, err := g.DiscoverAndConsumeServiceStream("myServiceName", "myStreamName")
if err != nil {
    panic(err)
}

for evt := range consumer.EvtChan() {
    fmt.Println(evt)
}
```


You will find more complete examples in the cmd folder

The gRPC port is configured with this property, it assigns a random port by default:
```
grpc.port=9666
```


### Tracing

Tracing is done through Zipkin, it can be enable with this property:
```
tracing.enabled=true
```

If service discovery is enabled, gorillaz will try to resolve the 'zipkin' service to find its endpoint.
Otherwise you can configure it with this property:
 ```
 tracing.collector.url=http://127.0.0.1:9411/api/v1/spans // if you do not configure zipkin with the service discovery
 ```

