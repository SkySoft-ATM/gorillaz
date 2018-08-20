package gorillaz

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	headers []kafka.Header
}

func InitTracing() {
	bootstrapServers := viper.GetString("kafka.bootstrapservers")
	collector, err := zipkintracer.NewKafkaCollector([]string{bootstrapServers},
		zipkintracer.KafkaTopic("tracing"))
	if err != nil {
		log.Fatalf("Unable to start Zipkin collector on server %v: %s", bootstrapServers, err)
		panic(err)
	}

	recorder := zipkintracer.NewRecorder(collector, false, "", "ms-asterix-airmap")
	tracer, err = zipkintracer.NewTracer(recorder)
	if err != nil {
		log.Fatalf("Unable to start Zipkin tracer: %s", err)
		panic(err)
	}
}

func (m *kafkaMessageWrapper) Set(key, val string) {
	if m.headers == nil {
		m.headers = make([]kafka.Header, 0)
	}

	m.headers = append(m.headers,
		kafka.Header{Key: strings.ToLower(key), Value: []byte(val)})
}

func (m *kafkaMessageWrapper) ForeachKey(handler func(key, val string) error) error {
	for _, v := range m.headers {
		handler(strings.ToLower(v.Key), string(v.Value))
	}

	return nil
}

func mapHeaders(headers []kafka.Header) opentracing.TextMapReader {
	return &kafkaMessageWrapper{headers}
}

func StartNewSpan(spanName string) opentracing.Span {
	span := tracer.StartSpan(spanName)
	return span
}

func StartSpan(spanName string, env KafkaEnvelope) opentracing.Span {
	header := env.Ctx.Value(KafkaHeaders).([]kafka.Header)
	reader := mapHeaders(header)

	ctx, err := tracer.Extract(opentracing.TextMap, reader)
	if err != nil {
		Log.Error("Error while extracting span information", zap.Error(err))

		return StartNewSpan(spanName)
	}

	span := tracer.StartSpan(spanName, opentracing.SpanReference{
		Type:              opentracing.FollowsFromRef,
		ReferencedContext: ctx,
	})

	return span
}

func inject(span opentracing.Span) []kafka.Header {
	wrapper := kafkaMessageWrapper{}
	err := tracer.Inject(span.Context(), opentracing.TextMap, &wrapper)
	if err != nil {
		Log.Error("Error while injecting Kafka headers", zap.Error(err))
	}
	return wrapper.headers
}

func Trace(span opentracing.Span, fields ...zlog.Field) {
	span.LogFields(fields...)
}

func FinishSpan(span opentracing.Span) {
	span.Finish()
}
