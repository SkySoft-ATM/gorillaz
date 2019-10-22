package main

import (
	"context"
	"flag"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/skysoft-atm/gorillaz"
	_ "net/http/pprof"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main() {
	var streamName string

	flag.StringVar(&streamName, "stream", "", "stream to receive")
	flag.Parse()

	g := gorillaz.New(gorillaz.WithConfigPath("./configs"))
	g.Run()

	opt := func(config *gorillaz.ProviderConfig) {
		config.SubscriberInputBufferLen = 1024
	}

	p, err := g.NewStreamProvider(streamName, "replace.with.proper.protobuf.type", opt)
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
			Ctx:   opentracing.ContextWithSpan(context.Background(), sp),
		}
		p.Submit(event)
		sp.Finish()
		message++
		time.Sleep(time.Nanosecond * 500)
	}
}
