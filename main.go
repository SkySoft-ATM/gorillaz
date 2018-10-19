package gorillaz

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

// Init initializes the different modules (Logger, Tracing, ready and live Probes and Properties)
// It takes root as a current folder for properties file and a map of properties
func Init(root string, context map[string]interface{}) {
	if root != "." {
		err := os.Chdir(root)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error trying to define ROOT directory")
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
}
