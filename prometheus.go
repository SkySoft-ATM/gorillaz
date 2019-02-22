package gorillaz

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"net/http"
	"runtime/debug"
	"strconv"
)

const prometheusEndpoint = "prometheus.endpoint"

func InitPrometheus() {
	go startPrometheusEndpoint()

}

func startPrometheusEndpoint() {
	defer recovery()
	endpoint := viper.GetString(prometheusEndpoint)
	port := viper.GetInt("http.port")
	Sugar.Infof("Starting Prometheus endpoint /%v on port %v", endpoint, strconv.Itoa(port))
	http.Handle("/"+endpoint, promhttp.Handler())
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
