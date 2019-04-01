package main

import (
	"flag"
	"fmt"
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

	var wg sync.WaitGroup
	wg.Add(clients)

	var worstLatency int64
	var totalLatency int64

	var ch chan *stream.Event
	var err error
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
	for i = 0; i < 10000; i++ {
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
