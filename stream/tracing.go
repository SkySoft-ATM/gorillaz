package stream

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	gaz "github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
	"time"
)

var tracer opentracing.Tracer

// OpenTracing TextMapWriter
func (m *Metadata) Set(key, value string) {
	m.KeyValue[key] = value
}

// OpenTracing TextMapReader
func (m *Metadata) ForeachKey(handler func(key, val string) error) error {
	for key, val := range m.KeyValue {
		err := handler(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func ctx(metadata *Metadata) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, timestampKey, metadata.StreamTimestamp)
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, metadata)
	if err != nil {
		return ctx
	}
	// Create the span referring to the RPC client if available.
	// If wireContext == nil, a root span will be created.
	serverSpan := opentracing.StartSpan("gorillaz.stream.consumer", ext.RPCServerOption(wireContext))
	serverSpan.Finish()
	ctx = opentracing.ContextWithSpan(ctx, serverSpan)
	return ctx
}

// metadata serialize evt.Context into a stream.Metadata with the tracing serialized as Text
func metadata(evt *Event) *Metadata {
	metadata := &Metadata{
		StreamTimestamp: time.Now().UnixNano(),
		KeyValue:        make(map[string]string),
	}

	if evt.Ctx != nil {
		sp := opentracing.SpanFromContext(evt.Ctx)
		if sp != nil {
			err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, metadata)
			if err != nil {
				gaz.Log.Error("cannot serialize tracing headers into the event", zap.Error(err))
			}
		}
	}
	return metadata
}
