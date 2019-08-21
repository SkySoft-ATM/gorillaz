package gorillaz

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InitPrometheus registers Prometheus handler to path to expose metrics via HTTP
func (g *Gaz) InitPrometheus(path string) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	g.prometheusRegistry = prometheus.NewRegistry()
	Sugar.Infof("Setup Prometheus handler at %s", path)
	g.Router.Handle(path, promhttp.Handler()).Methods("GET")

	// export uptime as a prometheus counter
	upCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "uptime_sec",
		Help: "uptime in seconds",
	})
	if g.RegisterCollector(upCounter) {
		go func() {
			t := time.Tick(time.Second)
			for {
				<-t
				upCounter.Inc()
			}
		}()
	}
}

// return true if collector was successfully registered
func (g *Gaz) RegisterCollector(c prometheus.Collector) bool {
	if g.prometheusRegistry != nil {
		err := g.prometheusRegistry.Register(c)
		if err != nil {
			Log.Warn("Could not register prometheus collector", zap.Error(err))
			return false
		}
		return true
	} else {
		Log.Info("No prometheus registry found")
		return false
	}
}
