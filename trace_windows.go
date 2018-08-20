package gorillaz

import (
	"github.com/opentracing/opentracing-go"
	zlog "github.com/opentracing/opentracing-go/log"
)

var tracer opentracing.Tracer

func init() {

}

func StartNewSpan(spanName string) opentracing.Span {
	return nil
}

func StartSpan(spanName string, env KafkaEnvelope) opentracing.Span {
	return nil
}

func Trace(span opentracing.Span, fields ...zlog.Field) {

}

func FinishSpan(span opentracing.Span) {

}
