package gorillaz

import (
	"bufio"
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"log"
	"os"
	"strings"
)

func getPropertiesKeys(scanner bufio.Scanner) map[string]string {
	m := make(map[string]string)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		split := strings.Split(line, "=")
		m[split[0]] = split[1]
	}

	return m
}

func makePropertiesKeysConfigurable(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		Log.Error("unable to open properties file", zap.String("file", filename))
	}
	scanner := bufio.NewScanner(f)
	m := getPropertiesKeys(*scanner)
	for k, v := range m {
		flag.String(k, v, "")
	}

	return nil
}

func parseConfiguration() {
	var conf string
	flag.StringVar(&conf, "conf", "configs", "config file. default: configs")
	flag.String("log.level", "", "Log level")
	makePropertiesKeysConfigurable(conf + "/application.properties")
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
