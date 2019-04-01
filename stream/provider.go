package stream

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	gaz "github.com/skysoft-atm/gorillaz"
	"github.com/skysoft-atm/gorillaz/mux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"sync"
)

var manager *subscriptionManager

func init() {
	server := grpc.NewServer()
	manager = &subscriptionManager{
		providers: make(map[string]*Provider),
		server:    server,
	}
	RegisterStreamServer(server, manager)
}

func Run(port int) error {
	gaz.Log.Info("listening on port", zap.Int("port", port))
	list, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go manager.server.Serve(list)
	return nil
}

type subscriptionManager struct {
	sync.RWMutex
	providers map[string]*Provider
	server    *grpc.Server
}

type Provider struct {
	name                string
	broadcaster         *mux.Broadcaster
	sentCounter         prometheus.Counter
	backPressureCounter prometheus.Counter
	clientCounter       prometheus.Gauge
	lastEventTimestamp  prometheus.Gauge
}

func (p *Provider) Submit(evt *Event) {
	streamEvent := &StreamEvent{
		Key:      evt.Key,
		Value:    evt.Value,
		Metadata: contextToMetadata(evt.Ctx),
	}
	p.sentCounter.Inc()
	p.lastEventTimestamp.SetToCurrentTime()
	p.broadcaster.SubmitBlocking(streamEvent)
}

func (p *Provider) Close() {
	gaz.Log.Info("closing stream", zap.String("stream", p.name))
	p.broadcaster.Close()
	manager.Lock()
	delete(manager.providers, p.name)
	manager.Unlock()
}

func NewProvider(name string) (*Provider, error) {
	gaz.Log.Info("creating stream", zap.String("stream", name))
	broadcaster, err := mux.NewNonBlockingBroadcaster(1024)
	if err != nil {
		gaz.Log.Error("could not create stream broadcaster", zap.Error(err))
		return nil, err
	}
	p := &Provider{
		name:        name,
		broadcaster: broadcaster,
	}
	manager.Lock()
	manager.providers[name] = p
	manager.Unlock()

	p.sentCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_sent",
		Help: "The total number of messages sent",
		ConstLabels: prometheus.Labels{
			"stream": name,
		},
	})

	p.backPressureCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_backpressure_dropped",
		Help: "The total number of messages dropped due to backpressure",
		ConstLabels: prometheus.Labels{
			"stream": name,
		},
	})

	p.clientCounter = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_clients",
		Help: "The total number of clients connected",
		ConstLabels: prometheus.Labels{
			"stream": name,
		},
	})

	p.lastEventTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_last_evt_timestamp",
		Help: "Timestamp of the last event produced",
		ConstLabels: prometheus.Labels{
			"stream": name,
		},
	})

	return p, nil
}

func (manager *subscriptionManager) Stream(req *StreamRequest, stream Stream_StreamServer) error {
	gaz.Log.Info("new stream consumer", zap.String("stream", req.Name))
	streamName := req.Name
	manager.RLock()
	provider, ok := manager.providers[req.Name]
	manager.RUnlock()
	if !ok {
		return fmt.Errorf("unknown stream %s", streamName)
	}
	provider.clientCounter.Inc()
	broadcaster := provider.broadcaster
	streamCh := make(chan interface{}, 256)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func(interface{}) {
			gaz.Log.Warn("backpressure applied, an event won't be delivered because it can't consume fast enough", zap.String("stream", streamName))
			provider.backPressureCounter.Inc()
		})
		return nil
	})

	for val := range streamCh {
		evt := val.(*StreamEvent)
		err := stream.Send(evt)
		if err != nil {
			gaz.Log.Info("consumer disconnected", zap.Error(err))
			broadcaster.Unregister(streamCh)
			break
		}
	}
	provider.clientCounter.Dec()
	return nil
}
