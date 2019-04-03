package main

import (
	"context"
	"flag"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/skysoft-atm/gorillaz"
	"net/http"
	_ "net/http/pprof"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main() {
	var streamName string

	var port int
	flag.StringVar(&streamName, "stream", "", "stream to receive")

	flag.IntVar(&port, "port", 0, "tcp port to listen to")
	flag.Parse()

	g := gorillaz.New(nil)
	g.Run()

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
		sp, _ := opentracing.StartSpanFromContext(context.Background(), "sending_message")
		sp.LogFields(log.Int64("message", message))

		v := []byte("something wonderful")
		event := &stream.Event{
			Value: v,
			//	Ctx:   ctx,
		}
		p.Submit(event)
		sp.Finish()
		message++
		time.Sleep(time.Nanosecond * 10000)
	}

	p.Close()

	time.Sleep(time.Second * 5)
}
