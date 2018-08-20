package gorillaz

import (
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
)

func parseConfiguration() {
	flag.String("conf", "configs", "config file. default: configs")
	flag.String("log.level", "", "Log level")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	viper.SetConfigName("application")
	configFile := viper.GetString("conf")
	viper.AddConfigPath(configFile)

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Unable to load configuration: %s", err)
	}
}
