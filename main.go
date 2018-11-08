package gorillaz

import (
	"log"
	"os"

	"github.com/spf13/viper"
)

var isAlreadyInitialized = false

// Init initializes the different modules (Logger, Tracing, ready and live Probes and Properties)
// It takes root as a current folder for properties file and a map of properties
func Init(root string, context map[string]interface{}) {
	if isAlreadyInitialized {
		return
	}

	if root != "." {
		err := os.Chdir(root)
		if err != nil {
			log.Fatalf("error trying to define root directory %v", err)
		}
	}

	parseConfiguration(context)
	InitLogs()

	tracing := viper.GetBool("tracing.enabled")
	if tracing {
		InitTracing()
	}

	health := viper.GetBool("healthcheck.enabled")
	if health {
		serverPort := viper.GetInt("healthcheck.port")
		InitHealthcheck(serverPort)
	}

	pprof := viper.GetBool("pprof.enabled")
	if pprof {
		serverPort := viper.GetInt("pprof.port")
		InitPprof(serverPort)
	}

	isAlreadyInitialized = true
}
