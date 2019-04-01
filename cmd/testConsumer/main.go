package main

import (
	"flag"
	"fmt"
	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"os"
	"strings"
	"sync"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main() {

	var streamName string
	var endpoints string
	var clients int

	flag.StringVar(&streamName, "stream", "", "stream to receive")
	flag.StringVar(&endpoints, "endpoints", "", "endpoint to connect to")
	flag.Parse()

	collector, err := zipkin.NewHTTPCollector("http://fakeurl.skysoft-atm.com")
	if err != nil {
		fmt.Printf("unable to create Zipkin HTTP collector: %+v\n", err)
		os.Exit(-1)
	}

	// create recorder.
	recorder := zipkin.NewRecorder(collector, true, "127.0.0.1:61001", "testConsumer")

	// create tracer.
	tracer, err := zipkin.NewTracer(
		recorder,
		zipkin.ClientServerSameSpan(true),
		zipkin.TraceID128Bit(true),
	)
	if err != nil {
		fmt.Printf("unable to create Zipkin tracer: %+v\n", err)
		os.Exit(-1)
	}

	// explicitly set our tracer to be the default tracer.
	opentracing.InitGlobalTracer(tracer)

	var wg sync.WaitGroup
	wg.Add(clients)

	var worstLatency int64
	var totalLatency int64

	var ch chan *stream.Event
	for {
		ch, err = stream.NewConsumer(streamName, strings.Split(endpoints, ",")...)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}

	fmt.Println("client created")

	var i int64

	start := time.Now()
	for i = 0; i < 100000; i++ {
		evt := <-ch
		latency := time.Now().UnixNano() - stream.StreamTimestamp(evt)
		if latency > worstLatency {
			worstLatency = latency
		}
		totalLatency += latency
	}
	end := time.Now()

	fmt.Printf("received %d message in %d s, (%d message/s), worst latency:%.3f ms, avg latency:%.3f ms\n", i, (end.UnixNano()-start.UnixNano())/1000000000, i*1000000000/(end.UnixNano()-start.UnixNano()), float64(worstLatency)/1000000.0, float64(totalLatency)/float64(i)/1000000.0)

	fmt.Println("finished")
}
