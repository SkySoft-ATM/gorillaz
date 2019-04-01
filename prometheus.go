package gorillaz

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InitPrometheus registers Prometheus handler to path to expose metrics via HTTP
func (g *Gaz) InitPrometheus(path string) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	Sugar.Infof("Setup Prometheus handler at %s", path)
	g.Router.Handle(path, promhttp.Handler()).Methods("GET")
}
