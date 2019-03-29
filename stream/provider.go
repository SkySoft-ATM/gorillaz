package stream

import (
	"fmt"
	"github.com/skysoft-atm/gorillaz/mux"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

var manager *subscriptionManager

type Event struct {
	Key,Value []byte
	StreamTimestamp uint64
}

func init() {
	server := grpc.NewServer()
	manager = &subscriptionManager{
		providers: make(map[string]*Provider),
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
	providers  map[string]*Provider
	server     *grpc.Server
}

type Provider struct {
	name string
	broadcaster *mux.Broadcaster
}

func (p *Provider) Submit(evt *Event){
	streamEvent := &StreamEvent{
		Key: evt.Key,
		Value: evt.Value,
		Stream_Timestamp_Ns: uint64(time.Now().UnixNano()),
	}
	p.broadcaster.SubmitBlocking(streamEvent)
}

func (p *Provider) Close(){
	p.broadcaster.Close()
	manager.Lock()
	delete(manager.providers, p.name)
	manager.Unlock()
}


func NewProvider(name string) (*Provider, error) {
	broadcaster, err := mux.NewNonBlockingBroadcaster(1024)
	if err != nil {
		return nil, err
	}
	p := &Provider{
		name: name,
		broadcaster: broadcaster,
	}
	manager.Lock()
	manager.providers[name] = p
	manager.Unlock()

	return p, nil
}

func (manager *subscriptionManager) Stream(req *StreamRequest, stream Stream_StreamServer) error {
	streamName := req.Name
	manager.RLock()
	provider, ok := manager.providers[req.Name]
	manager.RUnlock()
	if !ok {
		return fmt.Errorf("unknown stream %s", streamName)
	}
	broadcaster := provider.broadcaster
	streamCh:= make(chan interface{}, 256)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func (interface{}){
			//TODO: prometheus
		})
		return nil
	})

	for val := range streamCh {
		evt := val.(*StreamEvent)
		err := stream.Send(evt)
		if err != nil {
			broadcaster.Unregister(streamCh)
			return err
		}
	}
	log.Println("producer channel closed for stream " + streamName)
	return nil
}
