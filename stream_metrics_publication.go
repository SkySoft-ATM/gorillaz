package gorillaz

import (
	"time"

	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const MetricsStream = "gorillazMetrics"

func publishMetrics(g *Gaz, interval time.Duration) {
	streamProvider, err := g.NewStreamProvider(MetricsStream, "io_prometheus_client.MetricFamily", TracingDisabled)
	if err != nil {
		Log.Error("could not start stream metrics publication", zap.Error(err))
		return
	}
	ticker := time.NewTicker(interval)
	for range ticker.C {
		metrics, err := g.prometheusRegistry.Gather()
		if err != nil {
			Log.Warn("error gathering metrics", zap.Error(err))
			continue
		}
		metricsBatch := stream.Metrics{Metrics: metrics}
		payload, err := proto.Marshal(&metricsBatch)
		if err != nil {
			Log.Warn("Could not marshal metrics", zap.Error(err))
		}
		e := stream.Event{Value: payload}
		streamProvider.Submit(&e)
	}
}
