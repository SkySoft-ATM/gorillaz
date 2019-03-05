package gorillaz

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const prometheusEndpoint = "prometheus.endpoint"

// PromConfig is prometheus handler configuration
type PromConfig struct {
	Path string // URL path where to expose metrics
	Port int    // TCP port to start Prometheus server
}

// InitPrometheus starts a HTTP server on port conf.Port and exposes metrics on path conf.Path
func InitPrometheus(conf PromConfig) {
	go startPrometheusEndpoint(conf)
}

func startPrometheusEndpoint(conf PromConfig) {
	port := conf.Port
	path := conf.Path
	if !strings.HasPrefix("/", path) {
		path = "/" + path
	}
	Sugar.Infof("Starting Prometheus endpoint %s on port %d", path, port)
	http.Handle(path, promhttp.Handler())
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		Sugar.Warnf("Prometheus endpoint stopped%v", err)
	}
}
