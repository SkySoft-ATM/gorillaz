package gorillaz

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"sync"
)

const (
	StreamNameLabel           = "stream"
	StreamEventSent           = "stream_event_sent"
	StreamBackpressureDropped = "stream_backpressure_dropped"
	StreamConnectedClients    = "stream_connected_clients"
	StreamLastEventTimestamp  = "stream_last_evt_timestamp"
)

// NewStreamProvider returns a new provider ready to be used.
// only one instance of provider should be created for a given streamName
func (g *Gaz) NewStreamProvider(streamName, dataType string, opts ...ProviderConfigOpt) (*StreamProvider, error) {
	Log.Info("creating stream", zap.String("stream", streamName))

	config := defaultProviderConfig()
	for _, opt := range opts {
		opt(config)
	}

	var broadcaster *mux.Broadcaster

	if config.LazyBroadcast {
		broadcaster = mux.NewNonBlockingBroadcaster(config.InputBufferLen, mux.LazyBroadcast)
	} else {
		broadcaster = mux.NewNonBlockingBroadcaster(config.InputBufferLen)
	}
	p := &StreamProvider{
		streamDef:   &StreamDefinition{Name: streamName, DataType: dataType},
		config:      config,
		broadcaster: broadcaster,
		metrics:     pMetricHolder(g, streamName),
		gaz:         g,
	}
	g.streamRegistry.register(p)
	return p, nil
}

type StreamProvider struct {
	streamDef   *StreamDefinition
	config      *ProviderConfig
	broadcaster *mux.Broadcaster
	metrics     providerMetricsHolder
	gaz         *Gaz
}

func (p *StreamProvider) streamDefinition() *StreamDefinition {
	return p.streamDef
}

func (p *StreamProvider) streamType() stream.StreamType {
	return stream.StreamType_STREAM
}

var pMetricHolderMu sync.Mutex
var pMetrics = make(map[string]providerMetricsHolder)

func pMetricHolder(g *Gaz, streamName string) providerMetricsHolder {
	pMetricHolderMu.Lock()
	defer pMetricHolderMu.Unlock()
	if h, ok := pMetrics[streamName]; ok {
		return h
	}

	h := providerMetricsHolder{
		sentCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamEventSent,
			Help: "The total number of messages sent",
			ConstLabels: prometheus.Labels{
				StreamNameLabel: streamName,
			},
		}),

		backPressureCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamBackpressureDropped,
			Help: "The total number of messages dropped due to backpressure",
			ConstLabels: prometheus.Labels{
				StreamNameLabel: streamName,
			},
		}),

		clientCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: StreamConnectedClients,
			Help: "The total number of clients connected",
			ConstLabels: prometheus.Labels{
				StreamNameLabel: streamName,
			},
		}),

		lastEventTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: StreamLastEventTimestamp,
			Help: "Timestamp of the last event produced",
			ConstLabels: prometheus.Labels{
				StreamNameLabel: streamName,
			},
		}),
	}
	g.prometheusRegistry.MustRegister(h.sentCounter)
	g.prometheusRegistry.MustRegister(h.backPressureCounter)
	g.prometheusRegistry.MustRegister(h.clientCounter)
	g.prometheusRegistry.MustRegister(h.lastEventTimestamp)
	pMetrics[streamName] = h
	return h
}

type providerMetricsHolder struct {
	sentCounter         prometheus.Counter
	backPressureCounter prometheus.Counter
	clientCounter       prometheus.Gauge
	lastEventTimestamp  prometheus.Gauge
}

// ProviderConfig is the configuration that will be applied for the stream StreamProvider
type ProviderConfig struct {
	InputBufferLen           int                     // InputBufferLen is the size of the input channel (default: 256)
	SubscriberInputBufferLen int                     // SubscriberInputBufferLen is the size of the channel used to forward events to each client. (default: 256)
	OnBackPressure           func(streamName string) // OnBackPressure is the function called when a customer cannot consume fast enough and event are dropped. (default: log)
	LazyBroadcast            bool                    // if lazy broadcaster, then the provider doesn't consume messages as long as there is no consumer
	DisconnectOnBackPressure bool                    // if DisconnectOnBackpressure, in case of backpressure, disconnect the consumer
}

func defaultProviderConfig() *ProviderConfig {
	return &ProviderConfig{
		InputBufferLen:           256,
		SubscriberInputBufferLen: 256,
		OnBackPressure: func(streamName string) {
			Log.Warn("backpressure applied, an event won't be delivered because it can't consume fast enough", zap.String("stream", streamName))
		},
		LazyBroadcast: false,
	}
}

// ProviderConfigOpt is a ProviderConfig option function to modify the ProviderConfig used by the stream StreamProvider
type ProviderConfigOpt func(p *ProviderConfig)

var LazyBroadcast = func(p *ProviderConfig) {
	p.LazyBroadcast = true
}

// Submit pushes the event to all subscribers
func (p *StreamProvider) Submit(evt *stream.Event) {
	streamEvent := &stream.StreamEvent{
		Metadata: &stream.Metadata{
			KeyValue: make(map[string]string),
		},
	}
	err := stream.ContextToMetadata(evt.Ctx, streamEvent.Metadata)
	if err != nil {
		Log.Error("error while creating Metadata from event.Context", zap.Error(err))
	}
	streamEvent.Key = evt.Key
	streamEvent.Value = evt.Value

	p.metrics.sentCounter.Inc()
	p.metrics.lastEventTimestamp.SetToCurrentTime()

	b, err := proto.Marshal(streamEvent)
	if err != nil {
		Log.Error("error while marshaling stream.StreamEvent, cannot send event", zap.Error(err))
		return
	}
	p.broadcaster.SubmitBlocking(b)
}

func (p *StreamProvider) sendHelloMessage(strm grpc.ServerStream, peer Peer) error {
	gwe := stream.StreamEvent{
		Metadata: &stream.Metadata{
			KeyValue: make(map[string]string),
		},
	}
	evt, err := proto.Marshal(&gwe)
	if err != nil {
		Log.Error("Error while marshalling GetAndWatchEvent", zap.Error(err))
		return err
	}
	if err := strm.(grpc.ServerStream).SendMsg(evt); err != nil {
		Log.Info("consumer disconnected", zap.Error(err), zap.String("stream", p.streamDef.Name), zap.String("peer", peer.address), zap.String("peer service", peer.serviceName))
		return err
	}
	return nil
}

func (p *StreamProvider) sendLoop(strm grpc.ServerStream, peer Peer) error {
	streamName := p.streamDef.Name
	p.metrics.clientCounter.Inc()
	defer func() {
		p.metrics.clientCounter.Dec()
	}()
	broadcaster := p.broadcaster
	streamCh := make(chan interface{}, p.config.SubscriberInputBufferLen)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func(interface{}) {
			p.config.OnBackPressure(streamName)
			p.metrics.backPressureCounter.Inc()
		})
		if p.config.DisconnectOnBackPressure {
			config.DisconnectOnBackpressure()
		}
		return nil
	})

	defer func() {
		broadcaster.Unregister(streamCh)
	}()

	for {
		select {
		case val, ok := <-streamCh:
			if !ok {
				// if the broadcaster is closed, then there are no more values to be sent, there is no error
				if broadcaster.Closed() {
					return nil
				}
				// otherwise, the consumer gets disconnected because it's not consuming fast enough
				return status.Error(codes.DataLoss, "not consuming fast enough")
			}
			evt := val.([]byte)
			if err := strm.SendMsg(evt); err != nil {
				Log.Info("consumer disconnected", zap.Error(err), zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("peer service", peer.serviceName))
				return err
			}
		case <-strm.Context().Done():
			Log.Info("consumer disconnected", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("peer service", peer.serviceName))
			return strm.Context().Err()
		}
	}
}

func (p *StreamProvider) CloseStream() error {
	return p.gaz.closeStream(p)
}

func (p *StreamProvider) close() {
	p.broadcaster.Close()
}

func GetFullStreamName(serviceName, streamName string) string {
	return fmt.Sprintf("%s.%s", serviceName, streamName)
}

// returns the service name and stream name
func ParseStreamName(fullStreamName string) (string, string) {
	li := strings.LastIndex(fullStreamName, ".")
	if li >= 0 {
		return fullStreamName[:li], fullStreamName[li+1:]
	}
	return fullStreamName, ""
}

// binaryCodec takes the received binary data and directly returns it, without serializing it with proto.
// the main reason to use this is in case of 100s of subscribers, encode the data only once and just forward it without re-encoding it for each subscriber
type binaryCodec struct{}

// Marshal returns the wire format of v.
func (c *binaryCodec) Marshal(v interface{}) ([]byte, error) {
	var encoded, ok = v.([]byte)
	if ok {
		return encoded, nil
	}
	return proto.Marshal(v.(proto.Message))
}

// Unmarshal parses the wire format into v.
func (c *binaryCodec) Unmarshal(data []byte, v interface{}) error {
	evt, ok := v.(*stream.StreamRequest)
	if ok {
		return proto.Unmarshal(data, evt)
	}
	return proto.Unmarshal(data, v.(proto.Message))
}

// Name returns the name of the Codec implementation. The returned string
// will be used as part of content type in transmission.  The result must be
// static; the result cannot change between calls.
func (c *binaryCodec) String() string {
	return "binaryCodec"
}
