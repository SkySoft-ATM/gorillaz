package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
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

	go func(){
		http.ListenAndServe(":6060", nil)
	}()

	p, err := stream.NewProvider(streamName)
	if err != nil {
		panic(err)
	}


	err = stream.Run(port)
	if err != nil{
		panic(err)
	}

	tick := time.Tick(time.Microsecond*5)
	for i:=0;i<10000000;i++ {
		<- tick
		v := []byte(strconv.Itoa(i))
		event := &stream.Event{
			Value: v,
		}
		p.Submit(event)
	}

	p.Close()

	time.Sleep(time.Second*5)
}
