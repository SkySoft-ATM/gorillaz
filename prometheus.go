package gorillaz

import (
	"strings"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// InitPrometheus registers Prometheus handler to path to expose metrics via HTTP
func InitPrometheus(router *mux.Router, path string) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	Sugar.Infof("Setup Prometheus handler at %s", path)
	router.Handle(path, promhttp.Handler()).Methods("GET")
}
