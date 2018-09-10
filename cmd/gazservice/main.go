package main

import (
	"flag"
	"github.com/apex/log"
	"go/build"
	"os"
	"text/template"
)

type data struct {
	Package string
	Service string
	Source  string
	Sink    string
	Group   string
}

func main() {
	var d data
	flag.StringVar(&d.Service, "service", "", "Service name")
	flag.StringVar(&d.Source, "source", "kafka.source", "Kafka source configuration")
	flag.StringVar(&d.Sink, "sink", "kafka.sink", "Kafka sink configuration")
	flag.StringVar(&d.Group, "group", "groupid", "Kafka group id")
	flag.Parse()

	t := template.Must(template.New("service").Parse(serviceTemplate))
	path := d.Service + ".go"
	f, err := os.Create(path)
	if err != nil {
		log.Errorf("unable to create file %v", path)
		return
	}

	pkg, _ := build.Default.ImportDir(".", 0)

	d.Package = pkg.Name

	t.Execute(f, d)
	f.Close()
}

var serviceTemplate = `package {{.Package}}

import (
	gaz "github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
	"github.com/spf13/viper"
)

func create{{.Service}}Service()  {
	go gaz.KafkaService("",
		viper.GetString("{{.Source}}"),
		viper.GetString("{{.Sink}}"),
		"{{.Group}}",
		handler{{.Service}})
}

func handler{{.Service}}(request chan gaz.KafkaEnvelope, reply chan gaz.KafkaEnvelope) {
	for r := range request {
		gaz.Log.Debug("Handling {{.Service}}: %v", zap.ByteString("data", r.Data))
	}
}
`
