package gorillaz

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

var isAlreadyInitialized = false

// Init initializes the different modules (Logger, Tracing, ready and live Probes and Properties)
// It takes root as a current folder for properties file and a map of properties
func Init(root string, context map[string]interface{}) {
	if isAlreadyInitialized {
		return
	}

	//TODO: what's this for? We should be careful with creating folders, especially if this application runs as root user
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
