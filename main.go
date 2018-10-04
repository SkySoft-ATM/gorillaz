package gorillaz

import (
	"github.com/spf13/viper"
	"os"
)

type Context struct {
	chReady chan bool
	chLive  chan bool
}

func Init(root string, context map[string]interface{}) Context {
	if root != "." {
		os.Chdir(root)
	}

	ctx := Context{}

	parseConfiguration(context)
	InitLogs()

	tracing := viper.GetBool("tracing.enabled")
	if tracing {
		InitTracing()
	}

	health := viper.GetBool("healthcheck.enabled")
	if health {
		serverPort := viper.GetInt("healthcheck.port")
		chReady, chLive := InitHealthcheck(serverPort)
		ctx.chReady = chReady
		ctx.chLive = chLive
	}

	return ctx
}
