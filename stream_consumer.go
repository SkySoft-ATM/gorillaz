package gorillaz

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"math"
	"strings"
	"sync"
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
	StreamName() string
	EvtChan() chan *stream.Event
	Stop()
	streamEndpoint() *StreamEndpoint
}

type registeredConsumer struct {
	StreamConsumer
	g *Gaz
}

func (c *registeredConsumer) Stop() {
	c.StreamConsumer.Stop()
	c.g.stopConsumer(c)
}

type consumer struct {
	endpoint   *StreamEndpoint
	streamName string
	evtChan    chan *stream.Event
	config     *ConsumerConfig
	mu         sync.RWMutex
	stopped    bool
}

func (c *consumer) streamEndpoint() *StreamEndpoint {
	return c.endpoint
}

func (c *consumer) StreamName() string {
	return c.streamName
}

func (c *consumer) EvtChan() chan *stream.Event {
	return c.evtChan
}

func (c *consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
}

func (c *consumer) isStopped() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopped
}

type StreamEndpoint struct {
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

// Returns the stream endpoint for the given service name that will be discovered thanks to the service discovery mechanism
func (g *Gaz) NewServiceStreamEndpoint(serviceName string, opts ...StreamEndpointConfigOpt) (*StreamEndpoint, error) {
	return g.NewStreamEndpoint([]string{SdPrefix + serviceName})
}

func (g *Gaz) NewStreamEndpoint(endpoints []string, opts ...StreamEndpointConfigOpt) (*StreamEndpoint, error) {
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
	endpoint := &StreamEndpoint{
		config:    config,
		endpoints: endpoints,
		target:    target,
		conn:      conn,
	}
	return endpoint, nil
}

func (se *StreamEndpoint) Close() error {
	return se.conn.Close()
}

func (se *StreamEndpoint) ConsumeStream(streamName string, opts ...ConsumerConfigOpt) StreamConsumer {
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
	}

	var monitoringHolder = consumerMonitoring(streamName, se.endpoints)

	go func() {
		for se.conn.GetState() != connectivity.Shutdown && !c.isStopped() {
			waitTillReadyOrShutdown(streamName, se)
			if se.conn.GetState() == connectivity.Shutdown {
				break
			}

			client := stream.NewStreamClient(se.conn)
			req := &stream.StreamRequest{Name: streamName}

			var callOpts []grpc.CallOption
			if config.UseGzip {
				callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
			}
			st, err := client.Stream(context.Background(), req, callOpts...)
			if err != nil {
				Log.Warn("Error while creating stream", zap.String("stream", streamName), zap.Error(err))
				continue
			}

			//without this hack we do not know if the stream is really connected
			mds, err := st.Header()
			if err == nil && mds != nil {

				if config.OnConnected != nil {
					config.OnConnected(streamName)
				}
				Log.Debug("Stream connected", zap.String("streamName", streamName))

				// at this point, the GRPC connection is established with the server
				for !c.stopped {
					monitoringHolder.conGauge.Set(1)
					streamEvt, err := st.Recv()

					if err != nil {
						Log.Warn("received error on stream", zap.String("stream", c.streamName), zap.Error(err))
						if e, ok := status.FromError(err); ok {
							switch e.Code() {
							case codes.PermissionDenied, codes.ResourceExhausted, codes.Unavailable,
								codes.Unimplemented, codes.NotFound, codes.Unauthenticated, codes.Unknown:
								time.Sleep(5 * time.Second)
							}
						}
						break
					}

					Log.Debug("event received", zap.String("stream", streamName))
					monitorDelays(monitoringHolder, streamEvt)

					evt := &stream.Event{
						Key:   streamEvt.Key,
						Value: streamEvt.Value,
						Ctx:   stream.MetadataToContext(*streamEvt.Metadata),
					}
					c.evtChan <- evt
				}
			} else {
				Log.Warn("Stream created but not connected", zap.String("stream", streamName))
				time.Sleep(5 * time.Second)
			}
			monitoringHolder.conGauge.Set(0)
			if config.OnDisconnected != nil {
				config.OnDisconnected(streamName)
			}

		}
		Log.Info("Stream closed", zap.String("stream", c.streamName))
		close(c.evtChan)

	}()
	return c
}

func monitorDelays(monitoringHolder consumerMonitoringHolder, streamEvt *stream.StreamEvent) {
	monitoringHolder.receivedCounter.Inc()
	nowMs := float64(time.Now().UnixNano()) / 1000000.0
	streamTimestamp := streamEvt.Metadata.StreamTimestamp
	if streamTimestamp > 0 {
		// convert from ns to ms
		monitoringHolder.delaySummary.Observe(math.Max(0, nowMs-float64(streamTimestamp)/1000000.0))
	}
	eventTimestamp := streamEvt.Metadata.EventTimestamp
	if eventTimestamp > 0 {
		monitoringHolder.eventDelaySummary.Observe(math.Max(0, nowMs-float64(eventTimestamp)/1000000.0))
	}
	originTimestamp := streamEvt.Metadata.OriginStreamTimestamp
	if originTimestamp > 0 {
		monitoringHolder.originDelaySummary.Observe(math.Max(0, nowMs-float64(originTimestamp)/1000000.0))
	}
}

func waitTillReadyOrShutdown(streamName string, se *StreamEndpoint) {
	var state connectivity.State
	for state = se.conn.GetState(); state != connectivity.Ready && state != connectivity.Shutdown; state = se.conn.GetState() {
		Log.Debug("Waiting for stream endpoint connection to be ready", zap.Strings("endpoint", se.endpoints), zap.String("streamName", streamName), zap.String("state", state.String()))
		se.conn.WaitForStateChange(context.Background(), state)
	}
	if state == connectivity.Ready {
		Log.Debug("Stream endpoint is ready", zap.Strings("endpoint", se.endpoints), zap.String("streamName", streamName))
	}
	if state == connectivity.Shutdown {
		Log.Debug("Stream endpoint is in shutdown state", zap.Strings("endpoint", se.endpoints), zap.String("streamName", streamName))
	}
}

type consumerMonitoringHolder struct {
	receivedCounter    prometheus.Counter
	conCounter         prometheus.Counter
	conGauge           prometheus.Gauge
	delaySummary       prometheus.Summary
	originDelaySummary prometheus.Summary
	eventDelaySummary  prometheus.Summary
}

// map of metrics registered to Prometheus
// it's here because we cannot register twice to Prometheus the metrics with the same label
// if we register several consumers on the same stream, we must be sure we don't register the metrics twice
var consMonitoringMu sync.Mutex
var consumerMonitorings = make(map[string]consumerMonitoringHolder)

func consumerMonitoring(streamName string, endpoints []string) consumerMonitoringHolder {
	consMonitoringMu.Lock()
	defer consMonitoringMu.Unlock()

	if m, ok := consumerMonitorings[streamName]; ok {
		return m
	}
	m := consumerMonitoringHolder{
		receivedCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_received_events",
			Help: "The total number of events received",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		conCounter: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_consumer_connection_attempts",
			Help: "The total number of connections to the stream",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		conGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_consumer_connected",
			Help: "1 if connected, otherwise 0",
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		delaySummary: promauto.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_delay_ms",
			Help:       "distribution of delay between when messages are sent to from the consumer and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),

		originDelaySummary: promauto.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_origin_delay_ms",
			Help:       "distribution of delay between when messages were created by the first producer in the chain of streams, and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),
		eventDelaySummary: promauto.NewSummary(prometheus.SummaryOpts{
			Name:       "stream_consumer_event_delay_ms",
			Help:       "distribution of delay between when messages were created and when they are received, in milliseconds",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			ConstLabels: prometheus.Labels{
				"stream":    streamName,
				"endpoints": strings.Join(endpoints, ","),
			},
		}),
	}
	consumerMonitorings[streamName] = m
	return m
}

type gogoCodec struct{}

// Marshal returns the wire format of v.
func (c *gogoCodec) Marshal(v interface{}) ([]byte, error) {
	var req = v.(*stream.StreamRequest)
	return req.Marshal()
}

// Unmarshal parses the wire format into v.
func (c *gogoCodec) Unmarshal(data []byte, v interface{}) error {
	evt := v.(*stream.StreamEvent)
	return evt.Unmarshal(data)
}

func (c *gogoCodec) Name() string {
	return "gogoCodec"
}
