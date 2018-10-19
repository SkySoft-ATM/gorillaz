package main

import (
	"flag"
	"go/build"
	"os"
	"text/template"

	"github.com/apex/log"
)

type data struct {
	Package string
	Name    string
	Sink    string
}

func main() {
	var d data
	flag.StringVar(&d.Name, "name", "", "Name name")
	flag.StringVar(&d.Sink, "sink", "kafka.sink", "Kafka sink configuration")
	flag.Parse()

	t := template.Must(template.New("service").Parse(serviceTemplate))
	path := d.Name + ".go"
	f, err := os.Create(path)
	if err != nil {
		log.Errorf("unable to create file %v", path)
		return
	}

	pkg, _ := build.Default.ImportDir(".", 0)

	d.Package = pkg.Name

	err = t.Execute(f, d)
	if err != nil {
		log.Warnf("unable to create template")
	}

	f.Close()
}

var serviceTemplate = `package {{.Package}}

import (
	gaz "github.com/skysoft-atm/gorillaz"
	"github.com/spf13/viper"
)

func create{{.Name}}Producer() (chan gaz.KafkaEnvelope, error) {
	return gaz.KafkaProducer("",
		viper.GetString("{{.Sink}}"))
}
`
