package gorillaz

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/skysoft-atm/gorillaz/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"net"
	"strings"
	"sync"
)

const (
	StreamProviderTag = "streamProvider"
	streamDefinitions = "streamDefinitions"
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
	var err error

	if config.LazyBroadcast {
		broadcaster, err = mux.NewNonBlockingBroadcaster(config.InputBufferLen, mux.LazyBroadcast)
	} else {
		broadcaster, err = mux.NewNonBlockingBroadcaster(config.InputBufferLen)
	}
	if err != nil {
		Log.Error("could not create stream broadcaster", zap.Error(err))
		return nil, err
	}
	p := &StreamProvider{
		streamDef:   &stream.StreamDefinition{Name: streamName, DataType: dataType},
		config:      config,
		broadcaster: broadcaster,
		metrics:     pMetricHolder(g, streamName),
		gaz:         g,
	}
	g.streamRegistry.register(p)
	return p, nil
}

func (g *Gaz) CloseStream(streamName string) error {
	log.Info("closing stream", zap.String("stream", streamName))
	provider, ok := g.streamRegistry.find(streamName)
	if !ok {
		return fmt.Errorf("cannot find stream " + streamName)
	}
	g.streamRegistry.unregister(streamName)
	provider.close()
	return nil
}

type StreamProvider struct {
	streamDef   *stream.StreamDefinition
	config      *ProviderConfig
	broadcaster *mux.Broadcaster
	metrics     providerMetricsHolder
	gaz         *Gaz
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
			Name: "stream_event_sent",
			Help: "The total number of messages sent",
			ConstLabels: prometheus.Labels{
				"stream": streamName,
			},
		}),

		backPressureCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_backpressure_dropped",
			Help: "The total number of messages dropped due to backpressure",
			ConstLabels: prometheus.Labels{
				"stream": streamName,
			},
		}),

		clientCounter: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stream_connected_clients",
			Help: "The total number of clients connected",
			ConstLabels: prometheus.Labels{
				"stream": streamName,
			},
		}),

		lastEventTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stream_last_evt_timestamp",
			Help: "Timestamp of the last event produced",
			ConstLabels: prometheus.Labels{
				"stream": streamName,
			},
		}),
	}
	g.RegisterCollector(h.sentCounter)
	g.RegisterCollector(h.backPressureCounter)
	g.RegisterCollector(h.clientCounter)
	g.RegisterCollector(h.lastEventTimestamp)
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

func (p *StreamProvider) sendLoop(streamName string, strm stream.Stream_StreamServer, peer string) {
	p.metrics.clientCounter.Inc()
	broadcaster := p.broadcaster
	streamCh := make(chan interface{}, p.config.SubscriberInputBufferLen)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func(interface{}) {
			p.config.OnBackPressure(streamName)
			p.metrics.backPressureCounter.Inc()
		})
		return nil
	})

forloop:
	for {
		select {
		case val, ok := <-streamCh:
			if !ok {
				break forloop //channel closed
			}
			evt := val.([]byte)
			if err := strm.(grpc.ServerStream).SendMsg(evt); err != nil {
				Log.Info("consumer disconnected", zap.Error(err), zap.String("stream", streamName), zap.String("peer", peer))
				broadcaster.Unregister(streamCh)
				break forloop
			}
		case _ = <-strm.Context().Done():
			Log.Info("consumer disconnected", zap.String("stream", streamName), zap.String("peer", peer))
			broadcaster.Unregister(streamCh)
			break forloop

		}
	}
	p.metrics.clientCounter.Dec()
}

func (p *StreamProvider) CloseStream() error {
	return p.gaz.CloseStream(p.streamDef.Name)
}

func (p *StreamProvider) close() {
	p.broadcaster.Close()
}

func (g *Gaz) DiscoverStreamDefinitions(serviceName string) (StreamConsumer, error) {
	return g.DiscoverAndConsumeServiceStream(serviceName, streamDefinitions)
}

type streamRegistry struct {
	sync.RWMutex
	g          *Gaz
	providers  map[string]*StreamProvider
	serviceIds map[string]RegistrationHandle // for each stream a service is registered in the service discovery
}

func (sr *streamRegistry) createStreamDefinitionsStream() error {
	provider, err := sr.g.NewStreamProvider("streamDefinitions", "stream.StreamDefinition")
	if err != nil {
		return err
	}
	updates := make(chan *mux.StateUpdate, 100)
	err = sr.g.streamDefinitionsBroadcaster.Register(updates)
	if err != nil {
		return err
	}
	go sr.runStreamDefinitions(updates, provider)
	return nil
}

func (sr *streamRegistry) runStreamDefinitions(updates chan *mux.StateUpdate, provider *StreamProvider) {
	defer sr.g.streamDefinitionsBroadcaster.Unregister(updates)
	for {
		select {
		case u := <-updates:
			var sd *stream.StreamDefinition
			if u.UpdateType == mux.Delete {
				sd = &stream.StreamDefinition{Name: u.Value.(string), IsDelete: true}
			} else {
				sd = u.Value.(*stream.StreamDefinition)
			}
			bytes, err := proto.Marshal(sd)
			if err != nil {
				Log.Error("Error encoding stream definition", zap.Error(err))
			} else {
				se := stream.Event{
					Ctx:   nil,
					Key:   []byte(sd.Name),
					Value: bytes,
				}
				provider.Submit(&se)
			}
		}
	}
}

func (r *streamRegistry) find(streamName string) (*StreamProvider, bool) {
	r.RLock()
	p, ok := r.providers[streamName]
	r.RUnlock()
	return p, ok
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

type StreamDefinitions []*stream.StreamDefinition

func (r *streamRegistry) register(p *StreamProvider) {
	streamName := p.streamDef.Name
	r.Lock()
	defer r.Unlock()
	if _, found := r.providers[streamName]; found {
		panic("cannot register 2 providers with the same streamName: " + streamName)
	}
	r.providers[streamName] = p
	err := p.gaz.streamDefinitionsBroadcaster.Submit(streamName, p.streamDef)
	if err != nil {
		panic(err)
	}
}

func (r *streamRegistry) unregister(streamName string) {
	r.Lock()
	defer r.Unlock()
	p, ok := r.providers[streamName]
	if ok {
		delete(r.providers, streamName)
		p.gaz.streamDefinitionsBroadcaster.Delete(streamName)
	}
}

// Stream implements streaming.proto Stream.
// should not be called by the client
func (r *streamRegistry) Stream(req *stream.StreamRequest, strm stream.Stream_StreamServer) error {
	peer := GetGrpcClientAddress(strm.Context())
	Log.Info("new stream consumer", zap.String("stream", req.Name), zap.String("peer", peer), zap.String("requester", req.RequesterName))
	streamName := req.Name
	r.RLock()
	provider, ok := r.providers[req.Name]
	r.RUnlock()
	if !ok {
		Log.Warn("unknown stream", zap.String("stream", streamName), zap.String("peer", peer), zap.String("requester", req.RequesterName))
		return fmt.Errorf("unknown stream %s", streamName)
	}
	// we need to send some data because right now it is the only way to check on the client side if the stream connection is really established
	header := metadata.Pairs("name", streamName)
	err := strm.SendHeader(header)
	if err != nil {
		Log.Error("client might be disconnected %s", zap.Error(err), zap.String("peer", peer), zap.String("requester", req.RequesterName))
		return nil
	}
	provider.sendLoop(streamName, strm, peer)
	return nil
}

func GetGrpcClientAddress(ctx context.Context) string {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "no peer in context"
	}
	if pr.Addr == net.Addr(nil) {
		return "no address found"
	}
	return pr.Addr.String()
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
