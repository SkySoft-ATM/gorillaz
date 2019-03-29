package stream

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/skysoft-atm/gorillaz/mux"
	"google.golang.org/grpc"
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
	name                string
	broadcaster         *mux.Broadcaster
	sentCounter         prometheus.Counter
	backPressureCounter prometheus.Counter
	clientCounter       prometheus.Gauge
	lastEventTimestamp  prometheus.Gauge
}

func (p *Provider) Submit(evt *Event){
	streamEvent := &StreamEvent{
		Key: evt.Key,
		Value: evt.Value,
		Stream_Timestamp_Ns: uint64(time.Now().UnixNano()),
	}
	p.sentCounter.Inc()
	p.lastEventTimestamp.SetToCurrentTime()
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

	p.sentCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_sent",
		Help: "The total number of messages sent",
		ConstLabels:prometheus.Labels{
			"stream": name,
		},
	})

	p.backPressureCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_backpressure_dropped",
		Help: "The total number of messages dropped due to backpressure",
		ConstLabels:prometheus.Labels{
			"stream": name,
		},
	})

	p.clientCounter = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_clients",
		Help: "The total number of clients connected",
		ConstLabels:prometheus.Labels{
			"stream": name,
		},
	})

	p.lastEventTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_last_evt_timestamp",
		Help: "Timestamp of the last event produced",
		ConstLabels:prometheus.Labels{
			"stream": name,
		},
	})


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
	provider.clientCounter.Inc()
	broadcaster := provider.broadcaster
	streamCh:= make(chan interface{}, 256)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func (interface{}){
			provider.backPressureCounter.Inc()
		})
		return nil
	})

	for val := range streamCh {
		evt := val.(*StreamEvent)
		err := stream.Send(evt)
		if err != nil {
			broadcaster.Unregister(streamCh)
			break
		}
	}
	provider.clientCounter.Dec()
	return nil
}
