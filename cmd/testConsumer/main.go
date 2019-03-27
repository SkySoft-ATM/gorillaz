package main

import (
	"flag"
	"fmt"
	"strings"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main(){

	var streamName string
	var endpoints string

	flag.StringVar(&streamName, "stream", "", "stream to receive")
	flag.StringVar(&endpoints, "endpoints", "", "endpoint to connect to")
	flag.Parse()

	ch, err := stream.NewConsumer(streamName, strings.Split(endpoints, ",")...)
	if err != nil {
		panic(err)
	}

	fmt.Println("client created")


	for evt := range ch  {
		fmt.Printf("received msg with key %s, value %s\n", string(evt.Key), string(evt.Value))
	}

	fmt.Println("finished")

}
