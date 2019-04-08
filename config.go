package gorillaz

import (
	"bufio"
	"flag"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const multilineSeparator = "\\"

func parseProperties(reader io.Reader) map[string]string {
	scanner := bufio.NewScanner(reader)
	m := make(map[string]string)
	var multiline string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Comments
		if strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasSuffix(line, multilineSeparator) {
			multiline += strings.TrimSuffix(line, multilineSeparator)
			continue
		} else {
			if len(multiline) > 0 {
				line = multiline + line
				multiline = ""
			}
		}

		split := strings.Split(line, "=")
		if len(split) < 2 {
			log.Printf("WARN: cannot parse config line %s\n", line)
			continue
		}
		m[split[0]] = split[1]
	}
	return m
}

func parsePropertyFileAndSetFlags(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open properties file %v", filename)
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Printf("unable to close file %s, %+v", filename, err)
		}
	}()

	kv := parseProperties(f)

	for k, v := range kv {
		if f := flag.Lookup(k); f != nil {
			setFlagValue(k, v)
		} else {
			flag.String(k, v, "")
		}
	}
	return nil
}

func parseConfiguration(context map[string]interface{}) {
	// If parsing already done
	conf := GetConfigPath(context)

	flag.String("log.level", "", "Log level")
	flag.Bool("tracing.enabled", false, "Tracing enabled")
	flag.Bool("healthcheck.enabled", false, "Healthcheck enabled")
	flag.Bool("pprof.enabled", false, "Pprof enabled")
	flag.Int("pprof.port", 8081, "pprof port")
	flag.String("prometheus.endpoint", "/metrics", "Prometheus endpoint")
	flag.Bool("prometheus.enabled", true, "Prometheus enabled")
	flag.Int("http.port", 9000, "http port")

	err := parsePropertyFileAndSetFlags(path.Join(conf, "application.properties"))
	if err != nil {
		log.Fatalf("unable to read and extract key/value in %s: %v", conf+"/application.properties", err)
	}

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	err = viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatalf("unable to bind flags: %v", err)
	}

	if err != nil {
		log.Fatalf("Unable to load configuration: %s", err)
	}

	for k, v := range context {
		viper.Set(k, v)
	}
}

func GetConfigPath(context map[string]interface{}) string {
	if v, contains := context["conf"]; contains {
		conf := v.(string)
		return conf
	}
	if f := flag.Lookup("conf"); f != nil {
		return f.Value.String()
	}
	var conf string
	flag.StringVar(&conf, "conf", "configs", "config folder. default: configs")
	return conf
}

func getFlagValue(name string) string {
	result := ""
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == name {
			result = f.Value.String()
		}
	})
	return result
}

func setFlagValue(name string, value string) {
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name == name {
			err := f.Value.Set(value)
			if err != nil {
				log.Printf("Could not set value for flag %s : %s\n", name, err)
			}
		}
	})
}
