package main

import (
	"flag"
	"fmt"
	"strconv"
	"time"
)
import "github.com/skysoft-atm/gorillaz/stream"

func main(){

	var port int
	flag.IntVar(&port, "port", 0, "tcp port to listen to")
	flag.Parse()

	evt1 := make(chan *stream.Event)
	stream.RegisterProvider("stream1", evt1)

	evt2 := make(chan *stream.Event)
	stream.RegisterProvider("stream2", evt2)

	err := stream.Run(port)
	if err != nil{
		panic(err)
	}

	for i:=0;i<10000;i++ {
		fmt.Println("about to create event "+strconv.Itoa(i))
		event := &stream.Event{
			Key : []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf("value %d", i)),
		}
		if i % 2 == 0{
			evt1 <- event
		} else {
			evt2 <- event
		}
		time.Sleep(time.Millisecond*3)
	}

	close(evt1)
	close(evt2)

	time.Sleep(time.Second*5)
}
