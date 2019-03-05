package gorillaz

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

var initalized = false

// Init initializes the different modules (Logger, Tracing, ready and live Probes and Properties)
// It takes root as a current folder for properties file and a map of properties
func Init(root string, context map[string]interface{}) {
	if initalized {
		panic("gorillaz is already initialized")
	}
	initalized = true

	if root != "." {
		err := os.Chdir(root)
		if err != nil {
			log.Fatalf("error trying to define root directory %v", err)
		}
	}

	parseConfiguration(context)
	InitLogs(viper.GetString("log.level"))

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

		port := viper.GetInt("http.port")
		Sugar.Infof("Starting http server on port %d", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), router)
		if err != nil {
			Sugar.Errorf("Cannot start HTTP server on :%d, %+v", port, err)
			panic(err)
		}
	}()
}
