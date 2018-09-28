package gorillaz

import (
	"bufio"
	"flag"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
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
		log.Fatalf("Unable to open properties file %v", filename)
		return err
	}
	scanner := bufio.NewScanner(f)
	m := getPropertiesKeys(*scanner)
	for k, v := range m {
		flag.String(k, v, "")
	}

	return nil
}

func parseConfiguration(context map[string]interface{}) {
	// If parsing already done
	var conf string

	if v, contains := context["conf"]; contains {
		conf = v.(string)
	} else {
		flag.StringVar(&conf, "conf", "configs", "config file. default: configs")
	}

	flag.String("log.level", "", "Log level")
	makePropertiesKeysConfigurable(conf + "/application.properties")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	viper.SetConfigName("application")
	viper.AddConfigPath(conf)

	err := viper.ReadInConfig()

	if err != nil {
		log.Fatalf("Unable to load configuration: %s", err)
	}

	for k, v := range context {
		viper.Set(k, v)
	}
}
