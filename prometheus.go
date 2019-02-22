package gorillaz

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"net/http"
)

const prometheusEndpoint = "prometheus.endpoint"
const prometheusPort = "prometheus.port"

func InitPrometheus() {
	if !isFlagDefined(prometheusEndpoint) {
		flag.String(prometheusEndpoint, "metrics", "Prometheus endpoint")
	}

	if !isFlagDefined(prometheusPort) {
		flag.Int(prometheusPort, 9001, "Prometheus port")
	}
	endpoint := viper.GetString(prometheusEndpoint)
	port := viper.GetInt(prometheusPort)
	Sugar.Info("Starting prometheus endpoint /%v on port %v", endpoint, port)
	http.Handle("/"+endpoint, promhttp.Handler())
	http.ListenAndServe(":"+string(port), nil)

}
