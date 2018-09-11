package gorillaz

import "github.com/spf13/viper"

func Init() {
	parseConfiguration()
	InitLogs()

	tracing := viper.GetBool("tracing")
	if tracing {
		InitTracing()
	}
}
