package main

import (
	"flag"
	"strconv"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main(){
	var streamName string

	var port int
	flag.StringVar(&streamName, "stream", "", "stream to receive")

	flag.IntVar(&port, "port", 0, "tcp port to listen to")
	flag.Parse()

	evt1 := make(chan *stream.Event,10)
	stream.RegisterProvider(streamName, evt1)


	err := stream.Run(port)
	if err != nil{
		panic(err)
	}

	ticker := time.Tick(time.Nanosecond*2)
	for i:=0;i<100000;i++ {
		<- ticker
		v := []byte(strconv.Itoa(i))
		event := &stream.Event{
			Key : v,
			Value: v,
		}
		evt1 <- event
	}

	close(evt1)

	time.Sleep(time.Second*5)
}
