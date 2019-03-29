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
	flag.IntVar(&clients, "clients", 1, "number of clients to call the stream in parallel")
	flag.Parse()

	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func() {

			var worstLatency uint64
			var totalLatency uint64

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
			for evt := range ch {
				latency := uint64(time.Now().UnixNano()) - evt.StreamTimestamp
				if latency > worstLatency {
					worstLatency = latency
				}
				totalLatency += latency
				i++
			}
			end := time.Now()

			fmt.Printf("received %d message in %d s, (%d message/s), worst latency:%.3f ms, avg latency:%.3f ms\n", i, (end.UnixNano()-start.UnixNano())/1000000000, i*1000000000/(end.UnixNano()-start.UnixNano()), float64(worstLatency)/1000000.0, float64(totalLatency)/float64(i)/1000000.0)

			fmt.Println("finished")
			wg.Done()
		}()
	}
	wg.Wait()
}
