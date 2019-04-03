package main

import (
	"flag"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"strings"
	"time"
)

import gaz "github.com/skysoft-atm/gorillaz"

import "github.com/skysoft-atm/gorillaz/stream"

func main() {

	var streamName string
	var endpoints string
	var port int

	flag.StringVar(&streamName, "stream", "", "stream to connect to and produce")
	flag.StringVar(&endpoints, "endpoints", "", "endpoint to connect to")
	flag.IntVar(&port, "port", 0, "GRPC server")
	flag.Parse()

	g := gaz.New(nil)
	g.Run()

	var worstLatency int64
	var totalLatency int64

	// starting Grpc server
	stream.Run(port)
	p, err := stream.NewProvider(streamName)
	if err != nil {
		panic(err)
	}

	consumer, err := stream.NewConsumer(streamName, strings.Split(endpoints, ","))
	if err != nil {
		panic(err)
	}

	fmt.Println("client created")

	for {
		evt := <-consumer.EvtChan
		sp, _ := opentracing.StartSpanFromContext(evt.Ctx, "computing latency")
		latency := time.Now().UnixNano() - stream.StreamTimestamp(evt)
		if latency > worstLatency {
			worstLatency = latency
		}
		totalLatency += latency
		sp.Finish()
		p.Submit(evt)
	}
}
