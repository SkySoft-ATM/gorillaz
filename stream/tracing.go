package stream

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
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

func FillTracingSpan(e *Event, parent *Event) {
	if opentracing.SpanFromContext(e.ctx) == nil {
		// check if there is no span in the original msg
		if parent != nil {
			originalSpan := opentracing.SpanFromContext(parent.ctx)
			if originalSpan != nil {
				e.ctx = opentracing.ContextWithSpan(e.ctx, originalSpan)
			}
		}
	}
}

func EventMetadata(e *Event) (*Metadata, error) {
	ctx := e.ctx

	streamTs := time.Now().UnixNano()
	var eventTs int64
	var originStreamTs int64
	var eventType string
	var eventTypeVersion string
	var ts int64
	if ctx != nil {
		if ts := ctx.Value(eventTimeNs); ts != nil {
			eventTs = ts.(int64)
		}
		if ts := ctx.Value(originStreamTimestampNs); ts != nil {
			originStreamTs = ts.(int64)
		}
		if v := ctx.Value(eventTypeKey); v != nil {
			eventType = v.(string)
		}
		if v := ctx.Value(eventTypeVersionKey); v != nil {
			eventTypeVersion = v.(string)
		}
		if deadline, ok := ctx.Deadline(); ok {
			ts = deadline.UnixNano()
		}
	}

	if originStreamTs == 0 {
		originStreamTs = streamTs
	}

	metadata := &Metadata{
		KeyValue: make(map[string]string),
	}

	metadata.EventTimestamp = eventTs
	metadata.OriginStreamTimestamp = originStreamTs
	metadata.StreamTimestamp = streamTs
	metadata.EventType = eventType
	metadata.EventTypeVersion = eventTypeVersion
	metadata.Deadline = ts

	if ctx == nil {
		ctx = context.Background()
	}

	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		err := opentracing.GlobalTracer().Inject(sp.Context(), opentracing.TextMap, metadata)
		if err != nil {
			return nil, fmt.Errorf("cannot inject tracing headers in Metadata, %+v", err)
		}
	}
	return metadata, nil
}

func Ctx(metadata *Metadata) context.Context {
	ctx := context.Background()
	if metadata == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, eventTimeNs, metadata.EventTimestamp)
	ctx = context.WithValue(ctx, originStreamTimestampNs, metadata.OriginStreamTimestamp)
	ctx = context.WithValue(ctx, streamTimestampNs, metadata.StreamTimestamp)
	ctx = context.WithValue(ctx, eventTypeKey, metadata.EventType)
	ctx = context.WithValue(ctx, eventTypeVersionKey, metadata.EventTypeVersion)
	ctx = context.WithValue(ctx, deadlineKey, metadata.Deadline)

	spCtx, _ := opentracing.GlobalTracer().Extract(opentracing.TextMap, metadata)

	op := "gorillaz.stream.event.created"
	var span opentracing.Span
	if spCtx == nil {
		span = opentracing.StartSpan(op)
	} else {
		span = opentracing.StartSpan(op, opentracing.ChildOf(spCtx))
	}
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx
}

func NewEvent(ctx context.Context, key []byte, value []byte) *Event {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Event{
		ctx:     ctx,
		Key:     key,
		Value:   value,
		AckFunc: func() error { return nil },
	}
}
