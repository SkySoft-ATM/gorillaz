package gorillaz

import "github.com/spf13/viper"

func Init(context map[string]interface{}) {
	parseConfiguration(context)
	InitLogs()

	tracing := viper.GetBool("tracing.enabled")
	if tracing {
		InitTracing()
	}
}
