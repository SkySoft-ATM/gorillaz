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

// init function is automatically called when the package is imported
func init() {
	server := grpc.NewServer()
	manager = &subscriptionManager{
		providers: make(map[string]*Provider),
		server:    server,
	}
	RegisterStreamServer(server, manager)
}

// Run starts the stream GRPC endpoint on the given port.
func Run(port int) error {
	gaz.Log.Info("listening on port", zap.Int("port", port))
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	go manager.server.Serve(l)
	return nil
}

type Provider struct {
	streamName          string
	config              *ProviderConfig
	broadcaster         *mux.Broadcaster
	sentCounter         prometheus.Counter
	backPressureCounter prometheus.Counter
	clientCounter       prometheus.Gauge
	lastEventTimestamp  prometheus.Gauge
}

// ProviderConfig is the configuration that will be applied for the stream Provider
type ProviderConfig struct {
	InputBufferLen           int                     // InputBufferLen is the size of the input channel (default: 256)
	SubscriberInputBufferLen int                     // SubscriberInputBufferLen is the size of the channel used to forward events to each client. (default: 256)
	OnBackPressure           func(streamName string) // OnBackPressure is the function called when a customer cannot consume fast enough and event are dropped. (default: log)
}

func defaultProviderConfig() *ProviderConfig {
	return &ProviderConfig{
		InputBufferLen:           256,
		SubscriberInputBufferLen: 256,
		OnBackPressure: func(streamName string) {
			gaz.Log.Warn("backpressure applied, an event won't be delivered because it can't consume fast enough", zap.String("stream", streamName))
		},
	}
}

// ProviderConfigOpt is a ProviderConfig option function to modify the ProviderConfig used by the stream Provider
type ProviderConfigOpt func(p *ProviderConfig)

// NewProvider returns a new provider ready to be used.
// only one instance of provider should be created for a given streamName
func NewProvider(stremaName string, opts ...ProviderConfigOpt) (*Provider, error) {
	gaz.Log.Info("creating stream", zap.String("stream", stremaName))

	config := defaultProviderConfig()
	for _, opt := range opts {
		opt(config)
	}

	broadcaster, err := mux.NewNonBlockingBroadcaster(config.InputBufferLen)
	if err != nil {
		gaz.Log.Error("could not create stream broadcaster", zap.Error(err))
		return nil, err
	}
	p := &Provider{
		streamName:  stremaName,
		config:      config,
		broadcaster: broadcaster,
	}
	manager.Lock()
	manager.providers[stremaName] = p
	manager.Unlock()

	p.sentCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_event_sent",
		Help: "The total number of messages sent",
		ConstLabels: prometheus.Labels{
			"stream": stremaName,
		},
	})

	p.backPressureCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_backpressure_dropped",
		Help: "The total number of messages dropped due to backpressure",
		ConstLabels: prometheus.Labels{
			"stream": stremaName,
		},
	})

	p.clientCounter = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_connected_clients",
		Help: "The total number of clients connected",
		ConstLabels: prometheus.Labels{
			"stream": stremaName,
		},
	})

	p.lastEventTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_last_evt_timestamp",
		Help: "Timestamp of the last event produced",
		ConstLabels: prometheus.Labels{
			"stream": stremaName,
		},
	})

	return p, nil
}

// Submit pushes the event to all subscribers
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

// Close shuts down the publisher and releases the associated resources.
// Once a provider is closed, Submit should not be called.
func (p *Provider) Close() {
	gaz.Log.Info("closing stream", zap.String("stream", p.streamName))
	p.broadcaster.Close()
	manager.Lock()
	delete(manager.providers, p.streamName)
	manager.Unlock()
}

// Stream implements streaming.proto Stream.
// should not be called by the client
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
	streamCh := make(chan interface{}, provider.config.SubscriberInputBufferLen)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func(interface{}) {
			provider.config.OnBackPressure(streamName)
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

type subscriptionManager struct {
	sync.RWMutex
	providers map[string]*Provider
	server    *grpc.Server
}
