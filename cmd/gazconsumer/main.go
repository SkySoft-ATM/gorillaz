package main

import (
	"flag"
	"github.com/skysoft-atm/gorillaz"
	"go/build"
	"os"
	"text/template"
)

type data struct {
	Package string
	Name    string
	Source  string
	Sink    string
	Group   string
}

func main() {
	var d data
	flag.StringVar(&d.Name, "name", "", "Name name")
	flag.StringVar(&d.Source, "source", "kafka.source", "Kafka source configuration")
	flag.StringVar(&d.Group, "group", "groupid", "Kafka group id")
	flag.Parse()

	t := template.Must(template.New("service").Parse(serviceTemplate))
	path := d.Name + ".go"
	f, err := os.Create(path)
	if err != nil {
		gorillaz.Sugar.Errorf("unable to create file %v", path)
		return
	}

	pkg, _ := build.Default.ImportDir(".", 0)

	d.Package = pkg.Name

	err = t.Execute(f, d)
	if err != nil {
		gorillaz.Sugar.Warnf("unable to create template")
	}
	f.Close()
}

var serviceTemplate = `package {{.Package}}

import (
	gaz "github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
	"github.com/spf13/viper"
)

func create{{.Name}}Service()  {
	go gaz.KafkaConsumer("",
		viper.GetString("{{.Source}}"),
		"{{.Group}}",
		handler{{.Name}})
}

func handler{{.Name}}(request chan gaz.KafkaEnvelope) {
	for r := range request {
		gaz.Log.Debug("Handling {{.Name}}: %v", zap.ByteString("data", r.Data))
	}
}
`
