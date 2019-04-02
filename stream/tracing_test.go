package stream

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"testing"
)


func TestTimestamp(t *testing.T) {
	ts := int64(1234567890)

	metadata := &Metadata{
		StreamTimestamp: ts,
	}
	ctx := metadataToContext(metadata)
	evt := &Event{
		Ctx: ctx,
	}

	if StreamTimestamp(evt) != ts {
		t.Errorf("expected evt timestamp to be %d but is %d", ts, StreamTimestamp(evt))
	}
}

func TestTracingSerialization(t *testing.T){
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	span := opentracing.StartSpan("firstSpan")
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	span.Finish()

	metadata := contextToMetadata(ctx)
	ctx2 := metadataToContext(metadata)

	span2 := opentracing.SpanFromContext(ctx2)
	if span2 == nil {
		t.Errorf("span2 should not be nil")
		t.FailNow()
	}
}

func TestNoTracing(t *testing.T){
	tracer := mocktracer.New()
	opentracing.SetGlobalTracer(tracer)

	// a nil context should be serialized
	metadata := contextToMetadata(nil)
	ctx := metadataToContext(metadata)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		t.Errorf("span should be nil")
		t.FailNow()
	}

	// an empty context should be serializable
	metadata = contextToMetadata(context.Background())
	ctx = metadataToContext(metadata)

	span = opentracing.SpanFromContext(ctx)
	if span != nil {
		t.Errorf("span should be nil")
		t.FailNow()
	}
}