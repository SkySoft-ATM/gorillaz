package stream

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
)

func TestTimestamp(t *testing.T) {
	ts := int64(1234567890)

	metadata := &Metadata{
		StreamTimestamp: ts,
	}
	ctx := Ctx(metadata)

	evt := &Event{Ctx: ctx, Key: nil, Value: nil}

	if StreamTimestamp(evt) != ts {
		t.Errorf("expected evt timestamp to be %d but is %d", ts, StreamTimestamp(evt))
	}
}

func TestTracingSerialization(t *testing.T) {
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := opentracing.StartSpan("firstSpan")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	span.Finish()

	evt := &Event{Ctx: ctx}
	metadata, err := EventMetadata(evt)
	if err != nil {
		t.Errorf("failed to create event metadata from event, %+v", err)
		t.FailNow()
	}
	ctx2 := Ctx(metadata)
	evt = &Event{Ctx: ctx2}

	span2 := opentracing.SpanFromContext(evt.Ctx)
	if span2 == nil {
		t.Errorf("span2 should not be nil")
		t.FailNow()
	}
}
