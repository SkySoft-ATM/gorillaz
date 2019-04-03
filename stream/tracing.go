package stream

import (
	"context"
	"github.com/opentracing/opentracing-go"
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

func metadataToContext(metadata *Metadata) context.Context {
	ctx := context.WithValue(context.Background(), timestampKey, metadata.StreamTimestamp)
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, metadata)
	op := "gorillaz.stream.received"
	var span opentracing.Span

	// if no span is available, create a brand new one
	// otherwise, create a span with received span as parent
	if err != nil || wireContext == nil {
		span = opentracing.StartSpan(op)
	} else {
		span = opentracing.StartSpan(op, opentracing.ChildOf(wireContext))
	}
	span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx
}

// contextToMetadata serialize evt.Context into a stream.Metadata with the tracing serialized as Text
func contextToMetadata(ctx context.Context) *Metadata {
	metadata := &Metadata{
		StreamTimestamp: time.Now().UnixNano(),
		KeyValue:        make(map[string]string),
	}
	var sp opentracing.Span
	if ctx == nil {
		ctx = context.Background()
	}
	sp, ctx = opentracing.StartSpanFromContext(ctx, "gorillaz.stream.sending")
	sp.Finish()

	err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, metadata)
	if err != nil {
		gaz.Log.Error("cannot serialize tracing headers into the event", zap.Error(err))
	}
	return metadata
}
