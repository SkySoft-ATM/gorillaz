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
	router *mux.Router
}

// New initializes the different modules (Logger, Tracing, Metrics, ready and live Probes and Properties)
// It takes root at the current folder for properties file and a map of properties
func New(context map[string]interface{}) Gaz {
	if initialized {
		panic("gorillaz is already initialized")
	}
	initialized = true

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

	router := mux.NewRouter()

	return Gaz{router: router}
}

// Starts the router, once Run is launched, you should no longer add new handlers on the router
func (c Gaz) Run() {
	go func() {
		if health := viper.GetBool("healthcheck.enabled"); health {
			InitHealthcheck(c.router)
		}

		if prom := viper.GetBool("prometheus.enabled"); prom {
			promPath := viper.GetString("prometheus.endpoint")
			InitPrometheus(c.router, promPath)
		}

		if pprof := viper.GetBool("pprof.enabled"); pprof {
			InitPprof(viper.GetInt("pprof.port"))
		}

		// register /version to return the build version
		c.router.HandleFunc("/version", VersionHTML).Methods("GET")

		port := viper.GetInt("http.port")
		Sugar.Infof("Starting http server on port %d", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), c.router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", port, err)
			panic(err)
		}
	}()
}
