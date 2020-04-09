package gorillaz

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"io"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	// Prometheus metrics
	StreamConsumerReceivedEvents         = "stream_consumer_received_events"
	StreamConsumerConnectionAttempts     = "stream_consumer_connection_attempts"
	StreamConsumerConnectionStatusChecks = "stream_consumer_connection_status_checks"
	StreamConsumerConnectionStatus       = "stream_consumer_connection_status"
	StreamConsumerConnectionSuccess      = "stream_consumer_connection_success"
	StreamConsumerConnectionFailure      = "stream_consumer_connection_failure"
	StreamConsumerDisconnections         = "stream_consumer_disconnections"
	StreamConsumerConnected              = "stream_consumer_connected"
	StreamConsumerDelayMs                = "stream_consumer_delay_ms"
	StreamConsumerOriginDelayMs          = "stream_consumer_origin_delay_ms"
	StreamConsumerEventDelayMs           = "stream_consumer_event_delay_ms"
)

const StreamEndpointsLabel = "endpoints"

type StreamSourceConfig struct {
	BufferLen int // BufferLen is the size of the channel of the streamSource
	UseGzip   bool
}

type StreamConsumerConfig struct {
	*StreamSourceConfig
	StreamSubscriptionConfig
}

type StreamSubscriptionConfig interface {
	stream.SubscriptionConfig
	DisconnectOnBackpressure() bool
	SetDisconnectOnBackpressure()
}

type streamSubConf struct {
	stream.SubscriptionConfig
	disconnectOnBackpressure bool
}

func (s *streamSubConf) DisconnectOnBackpressure() bool {
	return s.disconnectOnBackpressure
}

func (s *streamSubConf) SetDisconnectOnBackpressure() {
	s.disconnectOnBackpressure = true
}

func DefaultSubscriptionOptions() StreamSubscriptionConfig {
	return &streamSubConf{
		SubscriptionConfig:       stream.DefaultSubscriptionOptions(),
		disconnectOnBackpressure: false,
	}
}

// subscription option that is only applicable to gorillaz streams
func DisconnectOnBackpressure() stream.SubscriptionOption {
	return func(s stream.SubscriptionConfig) {
		sc, ok := s.(StreamSubscriptionConfig)
		if !ok {
			panic("stream config is not a gorillaz stream config")
		}
		sc.SetDisconnectOnBackpressure()
	}
}

type StreamEndpointConfig struct {
	backoffMaxDelay time.Duration
}

type StreamConsumer interface {
	StreamName() string
	EvtChan() chan *stream.Event
	Stop()
}

type StoppableStream interface {
	Stop() bool
	StreamName() string
	streamEndpoint() *streamEndpoint
}

type streamSource struct {
	endpoint   *streamEndpoint
	streamName string
	config     *StreamSourceConfig
	cMetrics   *consumerMetrics
	unregister func()
}

func (c *streamSource) streamEndpoint() *streamEndpoint {
	return c.endpoint
}

func (c *streamSource) StreamName() string {
	return c.streamName
}

func (c *streamSource) metrics() *consumerMetrics {
	return c.cMetrics
}

type streamEndpoint struct {
	g         *Gaz
	target    string
	endpoints []string
	config    *StreamEndpointConfig
	conn      *grpc.ClientConn
}

func defaultStreamConsumerConfig() *StreamConsumerConfig {
	res := &StreamConsumerConfig{
		StreamSourceConfig: &StreamSourceConfig{
			BufferLen: 256,
			UseGzip:   false,
		},
		StreamSubscriptionConfig: DefaultSubscriptionOptions(),
	}
	res.StreamSubscriptionConfig.SetReconnectOnError(true)
	return res
}

func defaultConsumerConfig() *StreamSourceConfig {
	return &StreamSourceConfig{
		BufferLen: 256,
	}
}

func defaultStreamEndpointConfig() *StreamEndpointConfig {
	return &StreamEndpointConfig{
		backoffMaxDelay: 5 * time.Second,
	}
}

func BackoffMaxDelay(duration time.Duration) StreamEndpointConfigOpt {
	return func(config *StreamEndpointConfig) {
		config.backoffMaxDelay = duration
	}

}

type StreamSourceConfigOpt func(*StreamSourceConfig)

type StreamConsumerConfigOpt func(config *StreamConsumerConfig)

type StreamEndpointConfigOpt func(config *StreamEndpointConfig)

type EndpointType uint8

// Add options for the stream endpoint creation, this can be used when stream endpoints are created under the hood by the methods below.
func WithStreamEndpointOptions(opts ...StreamEndpointConfigOpt) Option {
	return Option{Opt: func(gaz *Gaz) error {
		gaz.streamEndpointOptions = opts
		return nil
	}}
}

// Call this method to create a stream streamSource with the full stream name (pattern: "serviceName.streamName")
// The service name is resolved via service discovery
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) DiscoverAndConsumeStream(fullStreamName string, opts ...StreamConsumerConfigOpt) (StreamConsumer, error) {
	srv, stream := ParseStreamName(fullStreamName)
	return g.DiscoverAndConsumeServiceStream(srv, stream, opts...)
}

// Call this method to create a stream streamSource
// The service name is resolved via service discovery
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) DiscoverAndConsumeServiceStream(service, stream string, opts ...StreamConsumerConfigOpt) (StreamConsumer, error) {
	return g.createConsumer([]string{SdPrefix + service}, stream, opts)
}

// Call this method to create a stream streamSource with the service endpoints and the stream name
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) ConsumeStream(endpoints []string, stream string, opts ...StreamConsumerConfigOpt) (StreamConsumer, error) {
	return g.createConsumer(endpoints, stream, opts)
}

type legacyConsumer struct {
	streamName string
	evtChan    chan *stream.Event
	cancel     context.CancelFunc
}

func (l legacyConsumer) StreamName() string {
	return l.streamName
}

func (l legacyConsumer) EvtChan() chan *stream.Event {
	return l.evtChan
}

func (l legacyConsumer) Stop() {
	l.cancel()
}

type channelSubscriber struct {
	streamName string
	evtChan    chan *stream.Event
}

func (cs channelSubscriber) OnNext(e *stream.Event) error {
	cs.evtChan <- e
	return nil
}

func (cs channelSubscriber) OnError(err error) {
	Log.Info("Error received on stream", zap.String("stream", cs.streamName), zap.Error(err))
}

func (cs channelSubscriber) OnComplete() {
	Log.Debug("Stream completed", zap.String("stream", cs.streamName))
}

func (g *Gaz) createConsumer(endpoints []string, streamName string, opts []StreamConsumerConfigOpt) (StreamConsumer, error) {
	config := defaultStreamConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	pub := StreamPublisher{
		endpoints:  endpoints,
		streamName: streamName,
		conf:       config.StreamSourceConfig,
		sr:         g.streamConsumers,
	}

	eChan := make(chan *stream.Event, config.BufferLen)
	ctx, cancel := context.WithCancel(context.Background())
	sub := channelSubscriber{
		streamName: streamName,
		evtChan:    eChan,
	}

	pub.subscribeWithConfig(ctx, sub, config.StreamSubscriptionConfig)

	return &legacyConsumer{
		streamName: streamName,
		evtChan:    eChan,
		cancel:     cancel,
	}, nil
}

func (g *Gaz) newStreamEndpoint(endpoints []string, opts ...StreamEndpointConfigOpt) (*streamEndpoint, error) {
	config := defaultStreamEndpointConfig()
	for _, opt := range opts {
		opt(config)
	}

	target := strings.Join(endpoints, ",")
	conn, err := g.GrpcDial(target, grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&gogoCodec{})),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 2 * time.Second,
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				MaxDelay:   config.backoffMaxDelay,
				Jitter:     0.2,
			},
		}),
	)

	if err != nil {
		return nil, err
	}
	endpoint := &streamEndpoint{
		g:         g,
		config:    config,
		endpoints: endpoints,
		target:    target,
		conn:      conn,
	}
	return endpoint, nil
}

func (se *streamEndpoint) close() error {
	return se.conn.Close()
}

type streamConsumerRegistry struct {
	sync.Mutex
	g                 *Gaz
	endpointsByName   map[string]*streamEndpoint
	endpointConsumers map[*streamEndpoint]map[*streamSource]struct{}
}

// this needs to be called with a lock on streamConsumerRegistry
func (r *streamConsumerRegistry) registerConsumer(e *streamEndpoint, ss *streamSource) {
	consumers := r.endpointConsumers[e]
	if consumers == nil {
		consumers = make(map[*streamSource]struct{})
		r.endpointConsumers[e] = consumers
	}
	consumers[ss] = struct{}{}
}

func (r *streamConsumerRegistry) unregisterConsumer(e *streamEndpoint, ss *streamSource) {
	r.Lock()
	defer r.Unlock()
	consumers, ok := r.endpointConsumers[e]
	if !ok {
		Log.Warn("Stream consumers not found", zap.String("stream name", ss.StreamName()),
			zap.String("target", e.target))
		return
	}
	delete(consumers, ss)
	if len(consumers) == 0 {
		Log.Info("Closing endpoint", zap.String("target", e.target))
		err := e.close()
		if err != nil {
			Log.Warn("Error while closing endpoint", zap.String("target", e.target), zap.Error(err))
		}
		delete(r.endpointsByName, e.target)
		delete(r.endpointConsumers, e)
	} else {
		r.endpointConsumers[e] = consumers
	}
}

func (r *streamConsumerRegistry) createStreamSource(sp *StreamPublisher) (*streamSource, error) {
	endpoints := sp.endpoints
	target := strings.Join(endpoints, ",")
	r.Lock()
	defer r.Unlock()
	e, ok := r.endpointsByName[target]
	if !ok {
		var err error
		Log.Debug("Creating stream endpoint", zap.String("target", target))
		e, err = r.g.newStreamEndpoint(endpoints, r.g.streamEndpointOptions...)
		if err != nil {
			return nil, errors.Wrapf(err, "error while creating stream endpoint for target %s", target)
		}
		r.endpointsByName[e.target] = e
	}
	ss := &streamSource{
		endpoint:   e,
		streamName: sp.streamName,
		config:     sp.conf,
		cMetrics:   consumerMonitoring(e.g, sp.streamName, target),
	}
	ss.unregister = func() {
		r.unregisterConsumer(e, ss)
	}
	r.registerConsumer(e, ss)
	return ss, nil
}

type StreamPublisher struct {
	endpoints  []string
	streamName string
	conf       *StreamSourceConfig
	sr         *streamConsumerRegistry
}

func ValueSubscriber(onNext func(*stream.Event) error) stream.Subscriber {
	return stream.CreateSubscriber(onNext, func(err error) {
		Log.Debug("Error received", zap.Error(err))
	}, func() {
		Log.Debug("Stream completed")
	})
}

func (g *Gaz) CreatePublisher(service, stream string, opts ...StreamSourceConfigOpt) StreamPublisher {
	return g.CreatePublisherForEndpoints([]string{SdPrefix + service}, stream, opts...)
}

func (g *Gaz) CreatePublisherForEndpoints(endpoints []string, streamName string, opts ...StreamSourceConfigOpt) StreamPublisher {
	config := defaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}
	return StreamPublisher{
		endpoints:  endpoints,
		streamName: streamName,
		conf:       config,
		sr:         g.streamConsumers,
	}
}

func (sp *StreamPublisher) Subscribe(ctx context.Context, sub stream.Subscriber, options ...stream.SubscriptionOption) {
	opt := DefaultSubscriptionOptions()
	for _, o := range options {
		o(opt)
	}

	sp.subscribeWithConfig(ctx, sub, opt)
}

func (sp *StreamPublisher) subscribeWithConfig(ctx context.Context, sub stream.Subscriber, conf StreamSubscriptionConfig) {
	go func() {
		retry := true
		for retry {
			retry = sp.createAndListenToSource(ctx, sub, conf)

			if retry {
				select {
				case <-ctx.Done():
					return
				default:
				}
			}

		}

	}()
}

func (sp *StreamPublisher) createAndListenToSource(ctx context.Context, sub stream.Subscriber, conf StreamSubscriptionConfig) (retry bool) {
	ss, err := sp.sr.createStreamSource(sp)
	if err != nil {
		if conf.ReconnectOnError() {
			return true
		} else {
			if conf.OnError() != nil {
				conf.OnError()(sp.streamName, err)
			}
			sub.OnError(err)
			return false
		}
	}
	defer ss.unregister()
	return ss.subscribe(ctx, sub, conf)
}

// returns true if we need to retry connecting
func (c *streamSource) subscribe(ctx context.Context, s stream.Subscriber, conf StreamSubscriptionConfig) (retry bool) {
	for waitTillConnReadyOrShutdown(ctx, c) == connectivity.Ready {
		err := c.consumeStream(ctx, s, conf)

		if err == nil { // stream completed
			if conf.ReconnectOnComplete() {
				Log.Debug("Stream completed, we will reconnect", zap.String("stream", c.streamName))
				time.Sleep(1 * time.Second) // TODO proper backoff
			} else {
				if conf.OnComplete() != nil {
					conf.OnComplete()(c.streamName)
				}
				s.OnComplete()
				return false
			}
		} else {
			if conf.OnError() != nil {
				conf.OnError()(c.streamName, err)
			}
			if conf.ReconnectOnError() {
				backOffOnError(c.streamName, c.endpoint.target, err)
			} else {
				s.OnError(err)
				return false
			}
		}
	}
	return conf.ReconnectOnError() // at this point the gRPC connection is in shutdown, we need to recreate it if we want to reconnect

}

func (c *streamSource) consumeStream(ctx context.Context, s stream.Subscriber, conf StreamSubscriptionConfig) error {
	c.cMetrics.conGauge.Set(0)
	c.cMetrics.conAttemptCounter.Inc()
	client := stream.NewStreamClient(c.streamEndpoint().conn)
	streamName := c.streamName
	requesterName := c.endpoint.g.ServiceName
	req := &stream.StreamRequest{
		Name:                     streamName,
		RequesterName:            requesterName,
		DisconnectOnBackpressure: conf.DisconnectOnBackpressure(),
	}

	var callOpts []grpc.CallOption
	if c.config.UseGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	st, err := client.Stream(streamCtx, req, callOpts...)

	if err != nil {
		c.cMetrics.failedConCounter.Inc()
		Log.Warn("Error while creating stream", zap.String("stream", streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		return err
	}
	//without this hack we do not know if the stream is really connected
	err = waitForHelloMessage(streamName, c.endpoint.target, st)
	if err != nil {
		c.cMetrics.conGauge.Set(0)
		c.cMetrics.failedConCounter.Inc()
		Log.Warn("Error while waiting for Hello message", zap.String("stream", streamName), zap.String("target", c.endpoint.target))
		if err == io.EOF {
			return nil
		} else {
			return err
		}
	}

	if conf.OnConnected() != nil {
		conf.OnConnected()(streamName)
	}
	Log.Info("Stream connected", zap.String("streamName", streamName), zap.String("target", c.endpoint.target))
	c.cMetrics.conGauge.Set(1)
	c.cMetrics.successConCounter.Inc()

	defer func() {
		if conf.OnDisconnected() != nil {
			conf.OnDisconnected()(streamName)
		}
	}()

	// at this point, the GRPC connection is established with the server
	for {
		streamEvt, err := st.Recv()
		if err != nil {
			c.cMetrics.conGauge.Set(0)
			c.cMetrics.disconnectionCounter.Inc()

			if err == io.EOF {
				return nil
			} else {
				if e, ok := status.FromError(err); ok && e.Code() == codes.Canceled {
					s.OnComplete() //ctx was canceled
				}
				return err
			}
		}

		if streamEvt == nil {
			Log.Warn("received a nil stream event", zap.String("stream", streamName), zap.String("target", c.endpoint.target))
			continue
		}
		if streamEvt.Metadata == nil {
			Log.Debug("received a nil stream.Metadata, creating an empty metadata", zap.String("stream", streamName), zap.String("target", c.endpoint.target))
			streamEvt.Metadata = &stream.Metadata{
				KeyValue: make(map[string]string),
			}
		}

		Log.Debug("event received", zap.String("stream", streamName), zap.String("target", c.endpoint.target))
		monitorDelays(c.cMetrics, streamEvt)

		evt := &stream.Event{
			Key:   streamEvt.Key,
			Value: streamEvt.Value,
			Ctx:   stream.MetadataToContext(*streamEvt.Metadata),
		}
		err = s.OnNext(evt)
		if err != nil {
			Log.Info("Could not push event", zap.Error(err), zap.String("stream", streamName), zap.String("client", requesterName))
		}
	}

}

func waitForHelloMessage(streamName, target string, st stream.Stream_StreamClient) error {
	Log.Debug("Waiting for Hello message", zap.String("stream", streamName), zap.String("target", target))
	_, err := st.Recv() //waiting for hello msg
	return err
}

func backOffOnError(streamName, target string, err error) {
	Log.Warn("received error on stream", zap.String("stream", streamName), zap.String("target", target), zap.Error(err))
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.PermissionDenied, codes.ResourceExhausted, codes.Unavailable,
			codes.Unimplemented, codes.NotFound, codes.Unauthenticated, codes.Unknown:
			time.Sleep(5 * time.Second)
		}
	}
}

type metadataProvider interface {
	GetMetadata() *stream.Metadata
}

func monitorDelays(metrics *consumerMetrics, evt metadataProvider) {
	metrics.receivedCounter.Inc()
	nowMs := float64(time.Now().UnixNano()) / 1000000.0
	metadata := evt.GetMetadata()
	streamTimestamp := metadata.StreamTimestamp
	if streamTimestamp > 0 {
		// convert from ns to ms
		metrics.delaySummary.Observe(math.Max(0, nowMs-float64(streamTimestamp)/1000000.0))
	}
	eventTimestamp := metadata.EventTimestamp
	if eventTimestamp > 0 {
		metrics.eventDelaySummary.Observe(math.Max(0, nowMs-float64(eventTimestamp)/1000000.0))
	}
	originTimestamp := metadata.OriginStreamTimestamp
	if originTimestamp > 0 {
		metrics.originDelaySummary.Observe(math.Max(0, nowMs-float64(originTimestamp)/1000000.0))
	}
}

func waitTillConnReadyOrShutdown(ctx context.Context, c *streamSource) connectivity.State {
	metrics := c.metrics()
	streamName := c.StreamName()
	conn := c.streamEndpoint().conn

	metrics.checkConnStatusCounter.Inc()
	var state = conn.GetState()
	metrics.connStatus.WithLabelValues(state.String()).Inc()

	for state != connectivity.Ready && state != connectivity.Shutdown {
		// count the number of connection status checks to know if a service has difficulties to establish a connection with a remote endpoint
		metrics.checkConnStatusCounter.Inc()

		Log.Debug("Waiting for stream endpoint connection to be ready", zap.Strings("endpoint", c.streamEndpoint().endpoints), zap.String("streamName", streamName), zap.String("state", state.String()))
		conn.WaitForStateChange(ctx, state)

		state = conn.GetState()
		metrics.connStatus.WithLabelValues(state.String()).Inc()
	}
	if state == connectivity.Ready {
		Log.Debug("Stream endpoint is ready", zap.Strings("endpoint", c.streamEndpoint().endpoints), zap.String("streamName", streamName))
	} else if state == connectivity.Shutdown {
		Log.Debug("Stream endpoint is in shutdown state", zap.Strings("endpoint", c.streamEndpoint().endpoints), zap.String("streamName", streamName))
	}
	return state
}

type consumerMetrics struct {
	receivedCounter        prometheus.Counter
	conAttemptCounter      prometheus.Counter
	checkConnStatusCounter prometheus.Counter
	connStatus             *prometheus.CounterVec
	disconnectionCounter   prometheus.Counter
	successConCounter      prometheus.Counter
	failedConCounter       prometheus.Counter
	conGauge               prometheus.Gauge
	delaySummary           prometheus.Summary
	originDelaySummary     prometheus.Summary
	eventDelaySummary      prometheus.Summary
}

// map of metrics registered to Prometheus
// it's here because we cannot register twice to Prometheus the metrics with the same label
// if we register several consumers on the same stream, we must be sure we don't register the metrics twice
var consumerMetricsMu sync.Mutex
var consumerMonitorings = make(map[string]*consumerMetrics)

func consumerMonitoring(g *Gaz, streamName string, target string) *consumerMetrics {
	consumerMetricsMu.Lock()
	defer consumerMetricsMu.Unlock()

	if m, ok := consumerMonitorings[streamName]; ok {
		return m
	}

	m := &consumerMetrics{
		receivedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerReceivedEvents,
			Help: "The total number of events received",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		conAttemptCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerConnectionAttempts,
			Help: "The total number of connections to the stream",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		checkConnStatusCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerConnectionStatusChecks,
			Help: "The total number of checks of gRPC connection status",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		connStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: StreamConsumerConnectionStatus,
			Help: "The total number of gRPC connection status",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}, []string{"status"}),

		successConCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerConnectionSuccess,
			Help: "The total number of successful connections to the stream",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		failedConCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerConnectionFailure,
			Help: "The total number of failed connection attempt to the stream",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		disconnectionCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: StreamConsumerDisconnections,
			Help: "The total number of disconnections to the stream",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		conGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: StreamConsumerConnected,
			Help: "1 if connected, otherwise 0",
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		delaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       StreamConsumerDelayMs,
			Help:       "distribution of delay between when messages are sent to from the streamSource and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),

		originDelaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       StreamConsumerOriginDelayMs,
			Help:       "distribution of delay between when messages were created by the first producer in the chain of streams, and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),
		eventDelaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       StreamConsumerEventDelayMs,
			Help:       "distribution of delay between when messages were created and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				StreamNameLabel:      streamName,
				StreamEndpointsLabel: target,
			},
		}),
	}
	g.prometheusRegistry.MustRegister(m.receivedCounter)
	g.prometheusRegistry.MustRegister(m.conAttemptCounter)
	g.prometheusRegistry.MustRegister(m.checkConnStatusCounter)
	g.prometheusRegistry.MustRegister(m.connStatus)
	g.prometheusRegistry.MustRegister(m.conGauge)
	g.prometheusRegistry.MustRegister(m.successConCounter)
	g.prometheusRegistry.MustRegister(m.disconnectionCounter)
	g.prometheusRegistry.MustRegister(m.failedConCounter)
	g.prometheusRegistry.MustRegister(m.delaySummary)
	g.prometheusRegistry.MustRegister(m.originDelaySummary)
	g.prometheusRegistry.MustRegister(m.eventDelaySummary)
	consumerMonitorings[streamName] = m
	return m
}

type gogoCodec struct{}

// Marshal returns the wire format of v.
func (c *gogoCodec) Marshal(v interface{}) ([]byte, error) {
	var req = v.(proto.Message)
	return proto.Marshal(req)
}

// Unmarshal parses the wire format into v.
func (c *gogoCodec) Unmarshal(data []byte, v interface{}) error {
	evt := v.(proto.Message)
	return proto.Unmarshal(data, evt)
}

func (c *gogoCodec) Name() string {
	return "gogoCodec"
}
