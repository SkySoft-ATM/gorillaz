package gorillaz

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	g.Router.Handle(path, promhttp.Handler()).Methods("GET")

	// export uptime as a prometheus counter
	upCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "uptime_sec",
		Help: "uptime in seconds",
	})

	go func() {
		t := time.Tick(time.Second)
		for {
			<-t
			upCounter.Inc()
		}
	}()
}
