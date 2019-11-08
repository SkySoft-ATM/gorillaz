package gorillaz

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ConsumerConfig struct {
	BufferLen      int // BufferLen is the size of the channel of the consumer
	OnConnected    func(streamName string)
	OnDisconnected func(streamName string)
	UseGzip        bool
}

type StreamEndpointConfig struct {
	backoffMaxDelay time.Duration
}

type StreamConsumer interface {
	streamConsumer
	EvtChan() chan *stream.Event
	Stop() bool //return previous 'stopped' state
}

type streamConsumer interface {
	metrics() *consumerMetrics
	StreamName() string
	streamEndpoint() *streamEndpoint
}

type StoppableStream interface {
	Stop() bool
	StreamName() string
	streamEndpoint() *streamEndpoint
}

type registeredConsumer struct {
	StreamConsumer
	g *Gaz
}

func (c *registeredConsumer) Stop() bool {
	wasAlreadyStopped := c.StreamConsumer.Stop()
	if wasAlreadyStopped {
		Log.Warn("Stop called twice", zap.String("stream name", c.StreamName()))
	} else {
		c.g.deregister(c)
	}
	return wasAlreadyStopped
}

type consumer struct {
	endpoint   *streamEndpoint
	streamName string
	evtChan    chan *stream.Event
	config     *ConsumerConfig
	stopped    *int32
	cMetrics   *consumerMetrics
}

func (c *consumer) streamEndpoint() *streamEndpoint {
	return c.endpoint
}

func (c *consumer) StreamName() string {
	return c.streamName
}

func (c *consumer) EvtChan() chan *stream.Event {
	return c.evtChan
}

func (c *consumer) Stop() bool {
	return atomic.SwapInt32(c.stopped, 1) == 1
}

func (c *consumer) isStopped() bool {
	return atomic.LoadInt32(c.stopped) == 1
}

func (c *consumer) metrics() *consumerMetrics {
	return c.cMetrics
}

type streamEndpoint struct {
	g         *Gaz
	target    string
	endpoints []string
	config    *StreamEndpointConfig
	conn      *grpc.ClientConn
}

func defaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
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

type ConsumerConfigOpt func(*ConsumerConfig)

type StreamEndpointConfigOpt func(config *StreamEndpointConfig)

type EndpointType uint8

// Add options for the stream endpoint creation, this can be used when stream endpoints are created under the hood by the methods below.
func WithStreamEndpointOptions(opts ...StreamEndpointConfigOpt) Option {
	return Option{Opt: func(gaz *Gaz) error {
		gaz.streamEndpointOptions = opts
		return nil
	}}
}

// Call this method to create a stream consumer with the full stream name (pattern: "serviceName.streamName")
// The service name is resolved via service discovery
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) DiscoverAndConsumeStream(fullStreamName string, opts ...ConsumerConfigOpt) (StreamConsumer, error) {
	srv, stream := ParseStreamName(fullStreamName)
	return g.DiscoverAndConsumeServiceStream(srv, stream, opts...)
}

// Call this method to create a stream consumer
// The service name is resolved via service discovery
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) DiscoverAndConsumeServiceStream(service, stream string, opts ...ConsumerConfigOpt) (StreamConsumer, error) {
	return g.createConsumer([]string{SdPrefix + service}, stream, opts...)
}

// Call this method to create a stream consumer with the service endpoints and the stream name
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) ConsumeStream(endpoints []string, stream string, opts ...ConsumerConfigOpt) (StreamConsumer, error) {
	return g.createConsumer(endpoints, stream, opts...)
}

func (g *Gaz) createConsumer(endpoints []string, streamName string, opts ...ConsumerConfigOpt) (StreamConsumer, error) {
	r := g.streamConsumers
	target := strings.Join(endpoints, ",")
	r.Lock()
	defer r.Unlock()
	e, ok := r.endpointsByName[target]
	if !ok {
		var err error
		Log.Debug("Creating stream endpoint", zap.String("target", target))
		e, err = r.g.newStreamEndpoint(endpoints, g.streamEndpointOptions...)
		if err != nil {
			return nil, errors.Wrapf(err, "error while creating stream endpoint for target %s", target)
		}
		r.endpointsByName[e.target] = e
	}
	sc := e.consumeStream(streamName, opts...)
	rc := registeredConsumer{g: r.g, StreamConsumer: sc}
	consumers := r.endpointConsumers[e]
	if consumers == nil {
		consumers = make(map[StoppableStream]struct{})
		r.endpointConsumers[e] = consumers
	}
	consumers[&rc] = struct{}{}
	return &rc, nil
}

func (g *Gaz) deregister(c StoppableStream) {
	r := g.streamConsumers
	e := c.streamEndpoint()
	r.Lock()
	defer r.Unlock()
	consumers, ok := r.endpointConsumers[e]
	if !ok {
		Log.Warn("Stream consumers not found", zap.String("stream name", c.StreamName()),
			zap.String("target", e.target))
		return
	}
	delete(consumers, c)
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

func (g *Gaz) newStreamEndpoint(endpoints []string, opts ...StreamEndpointConfigOpt) (*streamEndpoint, error) {
	config := defaultStreamEndpointConfig()
	for _, opt := range opts {
		opt(config)
	}

	target := strings.Join(endpoints, ",")
	conn, err := g.GrpcDial(target, grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(&gogoCodec{})),
		grpc.WithBackoffMaxDelay(config.backoffMaxDelay),
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

func (se *streamEndpoint) consumeStream(streamName string, opts ...ConsumerConfigOpt) StreamConsumer {
	config := defaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	ch := make(chan *stream.Event, config.BufferLen)

	c := &consumer{
		endpoint:   se,
		streamName: streamName,
		evtChan:    ch,
		config:     config,
		stopped:    new(int32),
		cMetrics:   consumerMonitoring(se.g, streamName, se.endpoints),
	}

	go func() {
		c.reconnectWhileNotStopped()
		Log.Info("Stream closed", zap.String("stream", c.streamName))
		close(c.evtChan)
	}()
	return c
}

func (c *consumer) reconnectWhileNotStopped() {
	for c.endpoint.conn.GetState() != connectivity.Shutdown && !c.isStopped() {
		c.cMetrics.conGauge.Set(0)
		c.cMetrics.conAttemptCounter.Inc()

		waitTillConnReadyOrShutdown(c)

		if c.endpoint.conn.GetState() == connectivity.Shutdown {
			break
		}
		retry := c.readStream()
		if !retry {
			break
		}
	}
}

func (c *consumer) readStream() (retry bool) {
	client := stream.NewStreamClient(c.endpoint.conn)
	req := &stream.StreamRequest{Name: c.streamName, RequesterName: c.endpoint.g.ServiceName, ExpectHello: true}

	var callOpts []grpc.CallOption
	if c.config.UseGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := client.Stream(ctx, req, callOpts...)
	if err != nil {
		c.cMetrics.failedConCounter.Inc()
		cancel()
		Log.Warn("Error while creating stream", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		return true
	}

	//without this hack we do not know if the stream is really connected
	mds, err := st.Header()
	if err == nil && mds != nil {
		var cs connectionStatus
		if mds.Get("expectHello") != nil && len(mds.Get("expectHello")) > 0 {
			cs = c.endpoint.waitForHelloMessage(c, c.streamName, st)
			if cs == closed {
				c.cMetrics.conGauge.Set(0)
				c.cMetrics.failedConCounter.Inc()
				Log.Warn("Stream closed after Hello message", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
				return false
			}
		} else {
			cs = connected
		}

		if cs == connected {
			if c.config.OnConnected != nil {
				c.config.OnConnected(c.streamName)
			}
			Log.Info("Stream connected", zap.String("streamName", c.streamName), zap.String("target", c.endpoint.target))
			c.cMetrics.conGauge.Set(1)
			c.cMetrics.successConCounter.Inc()

			// at this point, the GRPC connection is established with the server
			for !c.isStopped() {
				streamEvt, err := st.Recv()
				if err != nil {
					c.cMetrics.conGauge.Set(0)
					c.cMetrics.disconnectionCounter.Inc()

					if err == io.EOF {
						return false
					}
					c.backOffOnError(err)
					break
				}

				if streamEvt == nil {
					Log.Warn("received a nil stream event", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
					continue
				}
				if streamEvt.Metadata == nil {
					Log.Debug("received a nil stream.Metadata, creating an empty metadata", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
					streamEvt.Metadata = &stream.Metadata{
						KeyValue: make(map[string]string),
					}
				}

				Log.Debug("event received", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
				monitorDelays(c, streamEvt)

				evt := &stream.Event{
					Key:   streamEvt.Key,
					Value: streamEvt.Value,
					Ctx:   stream.MetadataToContext(*streamEvt.Metadata),
				}
				c.evtChan <- evt
			}
		}
	} else {
		c.cMetrics.conGauge.Set(0)
		c.cMetrics.failedConCounter.Inc()
		if mds == nil {
			Log.Warn("Stream created but not connected, no header received", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		} else {
			Log.Warn("Stream created but not connected", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		}
		time.Sleep(5 * time.Second)
	}
	if c.config.OnDisconnected != nil {
		c.config.OnDisconnected(c.streamName)
	}
	return true
}

type connectionStatus int

const (
	connected connectionStatus = iota
	notConnected
	closed
)

func (se *streamEndpoint) waitForHelloMessage(c *consumer, streamName string, st stream.Stream_StreamClient) connectionStatus {
	Log.Debug("Waiting for Hello message", zap.String("stream", streamName), zap.String("target", se.target))
	_, err := st.Recv() //waiting for hello msg
	if err == nil {
		return connected
	} else if err == io.EOF {
		return closed //standard error for closed stream
	} else {
		c.backOffOnError(err)
		return notConnected
	}
}

func (c *consumer) backOffOnError(err error) {
	Log.Warn("received error on stream", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
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

func monitorDelays(c streamConsumer, evt metadataProvider) {
	metrics := c.metrics()
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

func waitTillConnReadyOrShutdown(c streamConsumer) {
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn.WaitForStateChange(ctx, state)
		cancel()

		state = conn.GetState()
		metrics.connStatus.WithLabelValues(state.String()).Inc()
	}
	if state == connectivity.Ready {
		Log.Debug("Stream endpoint is ready", zap.Strings("endpoint", c.streamEndpoint().endpoints), zap.String("streamName", streamName))
		return
	}
	if state == connectivity.Shutdown {
		Log.Debug("Stream endpoint is in shutdown state", zap.Strings("endpoint", c.streamEndpoint().endpoints), zap.String("streamName", streamName))
		return
	}
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

func consumerMonitoring(g *Gaz, streamName string, endpoints []string) *consumerMetrics {
	consumerMetricsMu.Lock()
	defer consumerMetricsMu.Unlock()

	if m, ok := consumerMonitorings[streamName]; ok {
		return m
	}
	m := &consumerMetrics{
		receivedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_received_events",
			Help: "The total number of events received",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		conAttemptCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_connection_attempts",
			Help: "The total number of connections to the stream",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		checkConnStatusCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_connection_status_checks",
			Help: "The total number of checks of gRPC connection status",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		connStatus: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "stream_consumer_connection_status",
			Help: "The total number of gRPC connection status",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}, []string{"status"}),

		successConCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_connection_success",
			Help: "The total number of successful connections to the stream",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		failedConCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_connection_failure",
			Help: "The total number of failed connection attempt to the stream",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		disconnectionCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_disconnections",
			Help: "The total number of disconnections to the stream",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		conGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "stream_consumer_connected",
			Help: "1 if connected, otherwise 0",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		delaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_delay_ms",
			Help:       "distribution of delay between when messages are sent to from the consumer and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		originDelaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_origin_delay_ms",
			Help:       "distribution of delay between when messages were created by the first producer in the chain of streams, and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),
		eventDelaySummary: prometheus.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_event_delay_ms",
			Help:       "distribution of delay between when messages were created and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),
	}
	g.RegisterCollector(m.receivedCounter)
	g.RegisterCollector(m.conAttemptCounter)
	g.RegisterCollector(m.checkConnStatusCounter)
	g.RegisterCollector(m.connStatus)
	g.RegisterCollector(m.conGauge)
	g.RegisterCollector(m.successConCounter)
	g.RegisterCollector(m.disconnectionCounter)
	g.RegisterCollector(m.failedConCounter)
	g.RegisterCollector(m.delaySummary)
	g.RegisterCollector(m.originDelaySummary)
	g.RegisterCollector(m.eventDelaySummary)
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
