package gorillaz

import (
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InitPrometheus registers Prometheus handler to path to expose metrics via HTTP
func (g *Gaz) InitPrometheus(path string) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	Sugar.Infof("Setup Prometheus handler at %s", path)
	handler := promhttp.InstrumentMetricHandler(
		g.prometheusRegistry, promhttp.HandlerFor(g.prometheusRegistry, promhttp.HandlerOpts{}),
	)
	g.Router.Handle(path, handler).Methods("GET")

	// export uptime as a prometheus counter
	upCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "uptime_sec",
		Help: "uptime in seconds",
	})

	// register the application build version so we can collect it in Prometheus
	buildVersion := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "app_info",
		Help:        "application build information",
		ConstLabels: prometheus.Labels{"version": ApplicationVersion, "name": ApplicationName, "description": ApplicationDescription},
	})

	g.RegisterCollector(upCounter)
	g.RegisterCollector(buildVersion)

	// the actual value is set in "version" label
	buildVersion.Set(1)

	go func() {
		t := time.Tick(time.Second)
		for {
			<-t
			upCounter.Inc()
		}
	}()
}

// return true if collector was successfully registered
func (g *Gaz) RegisterCollector(c prometheus.Collector) error {
	err := g.prometheusRegistry.Register(c)
	if err != nil {
		return errors.Wrap(err, "Could not register prometheus collector")
	}
	return nil
}
