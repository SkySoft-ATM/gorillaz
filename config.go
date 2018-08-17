package gorillaz

import (
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
)

func ParseConfiguration() {
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
