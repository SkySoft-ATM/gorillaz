package gorillaz

import (
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	zlog "github.com/opentracing/opentracing-go/log"
	"github.com/openzipkin/zipkin-go-opentracing"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"strings"
)

var tracer opentracing.Tracer

type kafkaMessageWrapper struct {
	headers []sarama.RecordHeader
}

// InitTracing initializes Kafka connection to feed Zipkin
func InitTracing() {
	bootstrapServers := viper.GetString("kafka.bootstrapservers")
	tracingName := viper.GetString("tracing.service.name")

	collector, err := zipkintracer.NewKafkaCollector([]string{bootstrapServers},
		zipkintracer.KafkaTopic("tracing"))
	if err != nil {
		log.Fatalf("Unable to start Zipkin collector on server %v: %s", bootstrapServers, err)
		panic(err)
	}

	recorder := zipkintracer.NewRecorder(collector, false, "", tracingName)
	tracer, err = zipkintracer.NewTracer(recorder)
	if err != nil {
		log.Fatalf("Unable to start Zipkin tracer: %s", err)
		panic(err)
	}
}

// Set takes a key/value string pair and feel Kafka message headers
func (m *kafkaMessageWrapper) Set(key, val string) {
	if m.headers == nil {
		m.headers = make([]sarama.RecordHeader, 0)
	}

	m.headers = append(m.headers,
		sarama.RecordHeader{Key: []byte(strings.ToLower(key)), Value: []byte(val)})
}

// ForeachKey stringify and lower key/value pair and call handler function
func (m *kafkaMessageWrapper) ForeachKey(handler func(key, val string) error) error {
	for _, v := range m.headers {
		err := handler(strings.ToLower(string(v.Key)), string(v.Value))
		if err != nil {
			Sugar.Warnf("error trying to process key/value: %s/%s", string(v.Key), string(v.Value))
		}
	}

	return nil
}

func mapHeaders(headers []*sarama.RecordHeader) opentracing.TextMapReader {
	var h []sarama.RecordHeader
	for i := 0; i < len(headers); i++ {
		h = append(h, *headers[i])
	}
	return &kafkaMessageWrapper{h}
}

// StartNewSpan starts a new OpenTracing span with a given spanName
func StartNewSpan(spanName string) opentracing.Span {
	span := tracer.StartSpan(spanName)
	return span
}

func GetTraceId(span opentracing.Span) string {
	if span != nil && span.Context() != nil {
		return span.Context().(zipkintracer.SpanContext).TraceID.ToHex()
	}
	return ""
}

// StartSpan extracts info from kafka message headers and retrieve context
// BUG(teivah) Should be renamed in "Continue Span"
func StartSpan(spanName string, env KafkaEnvelope) opentracing.Span {
	header := env.Ctx.Value(KafkaHeaders).([]*sarama.RecordHeader)
	reader := mapHeaders(header)

	ctx, err := tracer.Extract(opentracing.TextMap, reader)
	if err != nil {
		Log.Error("Error while extracting span information", zap.Error(err))

		return StartNewSpan(spanName)
	}
	return createSpanFromContext(spanName, ctx)
}

func StartSpanFromExternalTraceId(spanName string, traceId string) opentracing.Span {
	if len(traceId) == 0 {
		return StartNewSpan(spanName)
	}

	var carrier = opentracing.TextMapCarrier(make(map[string]string))
	carrier.Set("x-b3-traceid", traceId)
	carrier.Set("x-b3-spanid", traceId)
	carrier.Set("x-b3-sampled", "true")

	ctx, err := tracer.Extract(opentracing.TextMap, carrier)
	if err != nil {
		Log.Warn("Error while creating context from traceId "+traceId+" we will create a new traceId", zap.Error(err))
		newSpan := StartNewSpan(spanName)
		newSpan = newSpan.SetTag("externalTraceId", traceId)
		return newSpan
	}

	return createSpanFromContext(spanName, ctx)
}

func createSpanFromContext(spanName string, ctx opentracing.SpanContext) opentracing.Span {
	span := tracer.StartSpan(spanName, opentracing.SpanReference{
		Type:              opentracing.FollowsFromRef,
		ReferencedContext: ctx,
	})
	return span
}

func inject(span opentracing.Span) []sarama.RecordHeader {
	wrapper := kafkaMessageWrapper{}
	err := tracer.Inject(span.Context(), opentracing.TextMap, &wrapper)
	if err != nil {
		Log.Error("Error while injecting Kafka headers", zap.Error(err))
	}
	return wrapper.headers
}

// Trace logs the given fields in the current span
func Trace(span opentracing.Span, fields ...zlog.Field) {
	span.LogFields(fields...)
}

// FinishSpan terminates the given span
func FinishSpan(span opentracing.Span) {
	span.Finish()
}
