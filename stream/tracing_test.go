package stream

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
)

func TestTimestamp(t *testing.T) {
	ts := int64(1234567890)

	metadata := Metadata{
		StreamTimestamp: ts,
	}
	ctx := MetadataToContext(&metadata)
	evt := &Event{
		Ctx: ctx,
	}

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

	metadata := Metadata{
		KeyValue: make(map[string]string),
	}

	err := ContextToMetadata(ctx, &metadata, "", false)
	if err != nil {
		t.Errorf("unexpected error: %+v", err)
	}
	ctx2 := MetadataToContext(&metadata)

	span2 := opentracing.SpanFromContext(ctx2)
	if span2 == nil {
		t.Errorf("span2 should not be nil")
		t.FailNow()
	}
}
