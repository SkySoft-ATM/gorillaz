package gorillaz

import (
	"net/http"
	"path"
	"runtime/debug"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const prometheusEndpoint = "prometheus.endpoint"

// Prometheus handler configuration
type PromConfig struct {
	Path string // URL path where to expose metrics
	Port int    // TCP port to start Prometheus server
}

// InitPrometheus starts a HTTP server on port conf.Port and exposes metrics on path conf.Path
func InitPrometheus(conf PromConfig) {
	go startPrometheusEndpoint(conf)
}

func startPrometheusEndpoint(conf PromConfig) {
	//TODO: can it panic?
	defer recovery()
	port := conf.Port
	endpoint := path.Join("/", conf.Path)
	Sugar.Infof("Starting Prometheus endpoint %s on port %d", endpoint, port)
	http.Handle(path.Join("/", endpoint), promhttp.Handler())
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		Sugar.Warnf("Prometheus endpoint stopped%v", err)
	}
}

func recovery() {
	if r := recover(); r != nil {
		Sugar.Warnf("Error while starting Prometheus endpoint %v", r)
		debug.PrintStack()
	}
}
