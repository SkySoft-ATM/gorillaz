package stream

import (
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"time"
)

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

func MetadataToContext(metadata Metadata) context.Context {
	ctx := context.WithValue(context.Background(), streamTimestampNs, metadata.StreamTimestamp)
	ctx = context.WithValue(ctx, originStreamTimestampNs, metadata.OriginStreamTimestamp)
	ctx = context.WithValue(ctx, eventTimeNs, metadata.EventTimestamp)
	wireContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, &metadata)
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
func ContextToMetadata(ctx context.Context, metadata *Metadata) error {
	streamTs := time.Now().UnixNano()
	var eventTs int64
	var originStreamTs int64
	if ctx != nil {
		if ts := ctx.Value(eventTimeNs); ts != nil {
			eventTs = ts.(int64)
		}
		if ts := ctx.Value(originStreamTimestampNs); ts != nil {
			originStreamTs = ts.(int64)
		}
	}

	if originStreamTs == 0 {
		originStreamTs = streamTs
	}
	metadata.EventTimestamp = eventTs
	metadata.OriginStreamTimestamp = originStreamTs
	metadata.StreamTimestamp = streamTs
	for key := range metadata.KeyValue {
		delete(metadata.KeyValue, key)
	}

	var sp opentracing.Span
	if ctx == nil {
		ctx = context.Background()
	}
	sp = opentracing.SpanFromContext(ctx)

	// create and close a span just to have a trace that a message was sent, it can always be useful
	if sp == nil {
		sp, _ = opentracing.StartSpanFromContext(ctx, "gorillaz.stream.sending")
		sp.Finish()
	}
	err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, metadata)
	if err != nil {
		err = fmt.Errorf("cannot inject tracing headers in Metadata, %+v", err)
	}
	return err
}
