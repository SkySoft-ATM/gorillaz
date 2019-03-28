package gorillaz

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var initalized = false

// Init initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func Init(context map[string]interface{}) {
	if initalized {
		panic("gorillaz is already initialized")
	}
	initalized = true

	parseConfiguration(context)
	err := InitLogs(viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if viper.GetBool("tracing.enabled") {
		InitTracing(
			KafkaTracingConfig{
				BootstrapServers: strings.Split(viper.GetString("kafka.bootstrapservers"), ","),
				TracingName:      viper.GetString("tracing.service.name"),
			})
	}

	go func() {
		router := mux.NewRouter()
		if health := viper.GetBool("healthcheck.enabled"); health {
			InitHealthcheck(router)
		}

		if prom := viper.GetBool("prometheus.enabled"); prom {
			promPath := viper.GetString("prometheus.endpoint")
			InitPrometheus(router, promPath)
		}

		if pprof := viper.GetBool("pprof.enabled"); pprof {
			InitPprof(viper.GetInt("pprof.port"))
		}

		// register /version to return the build version
		router.HandleFunc("/version", VersionHTML).Methods("GET")

		port := viper.GetInt("http.port")
		Sugar.Infof("Starting http server on port %d", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", port, err)
			panic(err)
		}
	}()
}
