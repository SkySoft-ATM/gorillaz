package stream

import (
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

var manager *subscriptionManager

type Event struct {
	Key, Value []byte
}

func init() {
	server := grpc.NewServer()
	manager = &subscriptionManager{
		subscriptions: make(map[string]*subscription),
		server:        server,
	}
	RegisterStreamServer(server, manager)
}

func Run(port int) error {
	list, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go manager.server.Serve(list)
	return nil
}

type subscriptionManager struct {
	sync.RWMutex
	subscriptions map[string]*subscription
	server        *grpc.Server
}

type subscription struct {
	sync.RWMutex
	inCh     chan *Event
	outChans map[chan *StreamEvent]struct{}
}


func RegisterProvider(name string, eventChan chan *Event){
	sub:= &subscription{
		inCh:     eventChan,
		outChans: make(map[chan *StreamEvent]struct{}),
	}
	manager.Lock()
	manager.subscriptions[name] = sub
	manager.Unlock()

	go func(sub *subscription){
		for evt := range eventChan{
			streamEvt := &StreamEvent{
				Key:evt.Key,
				Value: evt.Value,
			}
			sub.RLock()
			for out := range sub.outChans {
				select {
					case out <- streamEvt:
					//ok
					default:
					log.Println("backpressure for stream "+name)
				}
			}
			sub.RUnlock()
		}
		// if eventChan is closed, then close all subscriptions
		manager.Lock()
		sub.Lock()
		// disconnect consumers
		for c := range sub.outChans{
			close(c)
		}
		sub.Unlock()
		delete(manager.subscriptions, name)
		manager.Unlock()
	}(sub)
}



func (h *subscriptionManager) Stream(req *StreamRequest, stream Stream_StreamServer) error {
	streamName := req.Name
	manager.RLock()
	sub,ok := h.subscriptions[streamName]
	manager.RUnlock()
	if !ok{
		return fmt.Errorf("unknown stream %s", streamName)
	}
	out := make(chan *StreamEvent, 20)
	sub.Lock()
	sub.outChans[out] = struct{}{}
	sub.Unlock()

	for evt := range out {
		err := stream.Send(evt)
		if err != nil {
			// TODO: log error
			sub.Lock()
			delete(sub.outChans, out)
			sub.Unlock()
			return err
		}
	}
	log.Println("producer channel closed for stream "+streamName)
	return nil
}