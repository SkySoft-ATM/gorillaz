package stream

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"log"
)

func NewConsumer(streamName string, endpoints ...string) (chan *Event, error){
	// TODO: hacky hack to create a resolver to use with round robin
	r,_ := manual.GenerateAndRegisterManualResolver()

	addresses := make([]resolver.Address, len(endpoints))
	for i:=0;i<len(endpoints);i++{
		addresses[i] = resolver.Address{Addr: endpoints[i]}
	}
	r.InitialAddrs(addresses)

	conn, err := grpc.Dial(r.Scheme()+":///fake", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	if err != nil {
		return nil, err
	}
	c := NewStreamClient(conn)
	req := &StreamRequest{Name:streamName}
	stream, err := c.Stream(context.TODO(), req)
	if err != nil {
		return nil, err
	}
	ch := make(chan *Event)
	go func() {
		for {
			streamEvt, err := stream.Recv()
			if err != nil {
				log.Printf("ERROR: stream %s is unavailable, %v\n", streamName, err)
				close(ch)
				conn.Close()
				return
			}
			ch <- &Event{
				Key: streamEvt.Key,
				Value: streamEvt.Value,
			}
		}
	}()
	return ch, nil
}
