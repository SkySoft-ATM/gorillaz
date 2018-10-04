package gorillaz

import (
	"github.com/spf13/viper"
	"os"
)

func Init(root string, context map[string]interface{}) {
	if root != "." {
		os.Chdir(root)
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
		// Default port
		if serverPort == 0 {
			serverPort = 8080
		}
		InitHealthcheck(serverPort)
	}
}
