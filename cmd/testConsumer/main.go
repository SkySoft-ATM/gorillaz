package main

import (
	"flag"
	"fmt"
	"strings"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main(){

	var streamName string
	var endpoints string

	flag.StringVar(&streamName, "stream", "", "stream to receive")
	flag.StringVar(&endpoints, "endpoints", "", "endpoint to connect to")
	flag.Parse()

	var ch chan *stream.Event
	var err error
	for {
		ch, err = stream.NewConsumer(streamName, strings.Split(endpoints, ",")...)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond*200)
	}

	fmt.Println("client created")

	var i int64

	start := time.Now()
	for  range ch  {
		i++
	}
	end := time.Now()

	fmt.Printf("received %d message in %d s, (%d message/s)\n", i, (end.UnixNano() - start.UnixNano())/1000000000, i*1000000000/(end.UnixNano()-start.UnixNano()))

	fmt.Println("finished")

}
