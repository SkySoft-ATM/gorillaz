package gorillaz

import (
	"errors"
	"github.com/opentracing/opentracing-go"
	zlog "github.com/opentracing/opentracing-go/log"
	"github.com/skysoft-atm/zipkin-go-light-opentracing"
	"go.uber.org/zap"
	"log"
)

var tracer opentracing.Tracer

const traceIdKey = "x-b3-traceid"
const spanIdKey = "x-b3-spanid"

type TracingConfig struct {
	collectorUrl string
	tracingName  string
}

// InitTracingFromConfig initializes either an HTTP connection to a Zipkin Collector
// You should have provided the following configurations, either in the config file or with flags:
// zipkin.collector.url
func (g *Gaz) InitTracingFromConfig() {
	var collectorUrl string
	if g.ServiceDiscovery != nil {
		var err error
		collectorUrl, err = g.resolveZipkinUrlFromServiceDiscovery()
		if err != nil {
			Log.Info("Error while resolving zipkin from service discovery", zap.Error(err))
			collectorUrl = g.Viper.GetString("tracing.collector.url")
		} else if err != nil {
			Log.Info("No zipkin instance found in service discovery")
			collectorUrl = g.Viper.GetString("tracing.collector.url")
		}
	} else {
		collectorUrl = g.Viper.GetString("tracing.collector.url")
	}

	g.InitTracing(
		TracingConfig{
			collectorUrl: collectorUrl,
			tracingName:  g.ServiceName,
		})
}

func (g *Gaz) resolveZipkinUrlFromServiceDiscovery() (string, error) {
	tracingEndpoints, err := g.ResolveWithTag("zipkin", g.Env)
	if err != nil {
		return "", err
	}
	if len(tracingEndpoints) == 0 {
		return "", errors.New("No zipkin service found for env " + g.Env)
	}
	return tracingEndpoints[0].Meta["url"], nil
}

// InitTracing initializes connection to feed Zipkin
func (g *Gaz) InitTracing(conf TracingConfig) {

	if conf.collectorUrl == "" {
		panic("zipkin TracingConfig is invalid, collectorUrl is not set")
	}

	var collector zipkintracer.Collector
	var err error

	// TODO: should we crash the service at start time if the Zipkin collector is not available?
	Log.Info("connecting to Zipkin collector", zap.String("url", conf.collectorUrl), zap.String("tracing name", conf.tracingName))
	collector, err = zipkintracer.NewHTTPCollector(conf.collectorUrl)
	if err != nil {
		log.Fatal("cannot start connection to Zipkin collector endpoint", zap.Error(err))
	}

	recorder := zipkintracer.NewRecorder(collector, false, "", conf.tracingName)
	tracer, err = zipkintracer.NewTracer(
		recorder,
		zipkintracer.ClientServerSameSpan(true),
		zipkintracer.TraceID128Bit(true),
	)
	if err != nil {
		log.Fatalf("Unable to start Zipkin tracer: %s", err)
	}
	opentracing.SetGlobalTracer(tracer)
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

func StartSpanFromExternalTraceId(spanName string, traceId string) opentracing.Span {
	if len(traceId) == 0 {
		return StartNewSpan(spanName)
	}

	// TODO: this code makes me sad. Are we forced to use a SpanContext?

	var carrier = opentracing.TextMapCarrier(
		map[string]string{
			traceIdKey:     traceId,
			spanIdKey:      "0",
			"x-b3-sampled": "true",
		})
	ctx, err := tracer.Extract(opentracing.TextMap, carrier)
	if err != nil {
		newSpan := StartNewSpan(spanName)
		newSpan = newSpan.SetTag("externalTraceId", traceId)
		Log.Debug("Error while creating context from external traceId we created a new traceId", zap.Error(err),
			zap.String("external trace id", traceId), zap.String("new trace id", GetTraceId(newSpan)))
		return newSpan
	}
	return createSpanFromContext(spanName, ctx)
}

func ExtractTraceId(span opentracing.Span) string {
	var carrier = map[string]string{}

	if err := span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.TextMapCarrier(carrier)); err != nil {
		return ""
	}
	return carrier[traceIdKey]
}

func ExtractSpanId(span opentracing.Span) string {
	var carrier = map[string]string{}

	if err := span.Tracer().Inject(span.Context(), opentracing.HTTPHeaders, opentracing.TextMapCarrier(carrier)); err != nil {
		return ""
	}
	return carrier[spanIdKey]
}

func createSpanFromContext(spanName string, ctx opentracing.SpanContext) opentracing.Span {
	span := tracer.StartSpan(spanName, opentracing.SpanReference{
		Type:              opentracing.FollowsFromRef,
		ReferencedContext: ctx,
	})
	return span
}

// Trace logs the given fields in the current span
func Trace(span opentracing.Span, fields ...zlog.Field) {
	span.LogFields(fields...)
}

// FinishSpan terminates the given span
func FinishSpan(span opentracing.Span) {
	span.Finish()
}
