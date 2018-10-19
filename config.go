package gorillaz

import (
	"bufio"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func getPropertiesKeys(scanner bufio.Scanner) map[string]string {
	m := make(map[string]string)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Comments
		if !strings.HasPrefix("#", line) {
			split := strings.Split(line, "=")
			m[split[0]] = split[1]
		}
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
	flag.Bool("tracing.enabled", false, "Tracing enabled")
	flag.Bool("healthcheck.enabled", false, "Healthcheck enabled")
	flag.Int("healthcheck.port", 8080, "Healthcheck port")
	flag.Bool("pprof.enabled", false, "Pprof enabled")
	flag.Int("pprof.port", 8081, "Pprof port")

	err := makePropertiesKeysConfigurable(conf + "/application.properties")
	if err != nil {
		log.Fatalf("unable to read and extract key/value in %s: %v", conf+"/application.properties", err)
	}

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	err = viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatalf("unable to bind flags: %v", err)
	}
	viper.SetConfigName("application")
	viper.AddConfigPath(conf)

	err = viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Unable to load configuration: %s", err)
	}

	for k, v := range context {
		viper.Set(k, v)
	}

}
