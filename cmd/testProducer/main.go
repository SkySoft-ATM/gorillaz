package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	zipkin "github.com/openzipkin-contrib/zipkin-go-opentracing"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main() {
	var streamName string

	var port int
	flag.StringVar(&streamName, "stream", "", "stream to receive")

	flag.IntVar(&port, "port", 0, "tcp port to listen to")
	flag.Parse()

	// create collector.
	collector, err := zipkin.NewHTTPCollector("http://fakeurl.skysoft-atm.com")
	if err != nil {
		fmt.Printf("unable to create Zipkin HTTP collector: %+v\n", err)
		os.Exit(-1)
	}

	// create recorder.
	recorder := zipkin.NewRecorder(collector, true, "127.0.0.1:61001", "testProducer")

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

	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	opt := func(config *stream.ProviderConfig) {
		config.SubscriberInputBufferLen = 1024
	}

	p, err := stream.NewProvider(streamName, opt)
	if err != nil {
		panic(err)
	}

	err = stream.Run(port)
	if err != nil {
		panic(err)
	}

	var message int64
	for {
		ctx := context.Background()
		sp, ctx := opentracing.StartSpanFromContext(ctx, "sending_message")
		sp.LogFields(log.Int64("message", message))

		v := []byte("something wonderful")
		event := &stream.Event{
			Value: v,
			Ctx:   ctx,
		}
		sp.Finish()
		p.Submit(event)
		message++
		time.Sleep(time.Nanosecond * 100)
	}

	p.Close()

	time.Sleep(time.Second * 5)
}
