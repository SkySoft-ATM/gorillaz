package main

import (
	"flag"
	"fmt"
	"github.com/opentracing/opentracing-go"
	gaz "github.com/skysoft-atm/gorillaz"
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

	g := gaz.New(gaz.WithStreamEndpointOptions(gaz.BackoffMaxDelay(3*time.Second)), gaz.WithConfigPath("cmd/testFinalConsumer/configs"))
	g.Run()

	var wg sync.WaitGroup
	wg.Add(clients)

	var worstLatency int64
	var totalLatency int64

	consumer, err := g.ConsumeStream(strings.Split(endpoints, ","), streamName)
	if err != nil {
		panic(err)
	}

	fmt.Println("client created")

	var i int64

	var start time.Time
	for i = 0; i < 100000; i++ {
		if i == 0 {
			start = time.Now()
		}
		if i%1000 == 0 {
			fmt.Printf("consumed %d messages\n", i)
		}
		evt := <-consumer.EvtChan()
		ctx, cancel := evt.CtxWithDeadline()

		sp, _ := opentracing.StartSpanFromContext(ctx, "computing latency")
		latency := time.Now().UnixNano() - stream.StreamTimestamp(evt)
		if latency > worstLatency {
			worstLatency = latency
		}
		totalLatency += latency
		sp.Finish()
		cancel()
	}
	end := time.Now()

	fmt.Printf("received %d message in %d s, (%d message/s), worst latency:%.3f ms, avg latency:%.3f ms\n", i, (end.UnixNano()-start.UnixNano())/1000000000, i*1000000000/(end.UnixNano()-start.UnixNano()), float64(worstLatency)/1000000.0, float64(totalLatency)/float64(i)/1000000.0)

	fmt.Println("finished")
}
