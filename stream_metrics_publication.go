package gorillaz

import (
	"github.com/golang/protobuf/proto"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"time"
)

const MetricsStream = "gorillazMetrics"

func publishMetrics(g *Gaz) {
	streamProvider, err := g.NewStreamProvider(MetricsStream, "io_prometheus_client.MetricFamily")
	if err != nil {
		Log.Error("could not start stream metrics publication", zap.Error(err))
		return
	}
	interval := g.Viper.GetInt("metrics.publication.interval.ms")

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
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
