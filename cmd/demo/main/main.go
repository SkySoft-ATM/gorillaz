package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
)

func main() {
	g := gorillaz.New()
	gorillaz.ApplicationName = "adp-demo"
	gorillaz.ApplicationDescription = "this is wonderful"

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "hello_counter",
	})

	gorillaz.Log.Info("hello", zap.String("workd", "bla"))

	g.MustRegisterCollector(counter)

	g.Router.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		w.Write([]byte("hello world"))
	})

	select {}

}
