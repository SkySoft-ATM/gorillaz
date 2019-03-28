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
	unsubChan chan registration
	subChan   chan registration
	inCh      chan *Event
	outChans  map[chan *StreamEvent]struct{}
}

type registration struct {
	out  chan *StreamEvent
	done chan bool
}

func RegisterProvider(name string, eventChan chan *Event) {
	sub := &subscription{
		subChan:   make(chan registration),
		unsubChan: make(chan registration),
		inCh:      eventChan,
		outChans:  make(map[chan *StreamEvent]struct{}),
	}
	manager.Lock()
	manager.subscriptions[name] = sub
	manager.Unlock()

	go func(sub *subscription) {
		for {
			select {
			case reg := <-sub.subChan:
				sub.outChans[reg.out] = struct{}{}
				reg.done <- true

			case unreg := <-sub.unsubChan:
				delete(sub.outChans, unreg.out)
				unreg.done <- true

			case evt, ok := <-eventChan:
				if !ok {
					// eventChan is closed, cleanup
					// disconnect consumers
					for c := range sub.outChans {
						close(c)
					}
					manager.Lock()
					delete(manager.subscriptions, name)
					manager.Unlock()
					return
				}
				if len(sub.outChans) == 0 {
					break
				}
				streamEvt := &StreamEvent{
					Key:   evt.Key,
					Value: evt.Value,
				}

				for out := range sub.outChans {
					select {
					case out <- streamEvt:
						//ok
					default:
						log.Println("backpressure for stream " + name)
					}
				}
			}
		}
	}(sub)
}

func (h *subscriptionManager) Stream(req *StreamRequest, stream Stream_StreamServer) error {
	streamName := req.Name
	manager.RLock()
	sub, ok := h.subscriptions[streamName]
	manager.RUnlock()
	if !ok {
		return fmt.Errorf("unknown stream %s", streamName)
	}
	out := make(chan *StreamEvent, 256)
	registred := make(chan bool)
	sub.subChan <- registration{
		out:  out,
		done: registred,
	}
	<-registred

	for evt := range out {
		err := stream.Send(evt)
		if err != nil {
			unregistred := make(chan bool)
			sub.unsubChan <- registration{
				out:  out,
				done: unregistred,
			}
			<-unregistred
			return err
		}
	}
	log.Println("producer channel closed for stream " + streamName)
	return nil
}
