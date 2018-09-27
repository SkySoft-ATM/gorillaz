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
}
