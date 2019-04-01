package gorillaz

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var initialized = false

type Gaz struct {
	Router *mux.Router
	// use int32 because sync.atomic package doesn't support boolean out of the box
	isReady *int32
	isLive  *int32
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(context map[string]interface{}) *Gaz {
	if initialized {
		panic("gorillaz is already initialized")
	}
	initialized = true

	parseConfiguration(context)
	gaz := Gaz{Router: mux.NewRouter(), isReady: new(int32), isLive: new(int32)}
	err := gaz.InitLogs(viper.GetString("log.level"))
	if err != nil {
		panic(err)
	}

	if viper.GetBool("tracing.enabled") {
		gaz.InitTracing(
			KafkaTracingConfig{
				BootstrapServers: strings.Split(viper.GetString("kafka.bootstrapservers"), ","),
				TracingName:      viper.GetString("tracing.service.name"),
			})
	}

	return &gaz
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (g Gaz) Run() {
	go func() {
		if health := viper.GetBool("healthcheck.enabled"); health {
			Sugar.Info("Activating health check")
			g.InitHealthcheck()
		}

		if prom := viper.GetBool("prometheus.enabled"); prom {
			promPath := viper.GetString("prometheus.endpoint")
			g.InitPrometheus(promPath)
		}

		if pprof := viper.GetBool("pprof.enabled"); pprof {
			g.InitPprof(viper.GetInt("pprof.port"))
		}

		// register /version to return the build version
		g.Router.HandleFunc("/version", VersionHTML).Methods("GET")

		port := viper.GetInt("http.port")
		Sugar.Infof("Starting http server on port %d", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), g.Router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", port, err)
			panic(err)
		}
	}()
}
