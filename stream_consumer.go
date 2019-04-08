package gorillaz

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.RWMutex
var authority string

type ConsumerConfig struct {
	BufferLen         int                                     // BufferLen is the size of the channel of the consumer
	onConnectionRetry func(streamName string, retryNb uint64) // onConnectionRetry is called before trying to reconnect to a stream provider
	onConnected       func(streamName string)
	onDisconnected    func(streamName string)
	UseGzip           bool
}

type Consumer struct {
	StreamName string
	EvtChan    chan *stream.Event
	target     string
	endpoints  []string
	config     *ConsumerConfig
}

func defaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BufferLen: 256,
		onConnectionRetry: func(streamName string, attemptNb uint64) {
			wait := time.Second * 0
			switch attemptNb {
			case 0:
				// just try to connect directly on the first attempt
				break
			case 1:
				wait = time.Second * 1
			case 2:
				wait = time.Second * 2
			case 3:
				wait = time.Second * 3
			default:
				wait = time.Second * 5
			}
			if wait > 0 {
				Log.Info("waiting before making another connection attempt", zap.String("streamName", streamName), zap.Int("wait_sec", int(wait.Seconds())))
				time.Sleep(wait)
			}
		},
	}
}

type ConsumerConfigOpt func(*ConsumerConfig)

type EndpointType uint8

const (
	DNSEndpoint = EndpointType(iota)
	IPEndpoint
)

func NewStreamConsumer(streamName string, endpointType EndpointType, endpoints []string, opts ...ConsumerConfigOpt) (*Consumer, error) {
	// TODO: hacky hack to create a resolver to use with round robin
	mu.Lock()
	r, _ := manual.GenerateAndRegisterManualResolver()
	mu.Unlock()

	addresses := make([]resolver.Address, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		addresses[i] = resolver.Address{Addr: endpoints[i]}
	}
	r.InitialAddrs(addresses)
	target := r.Scheme() + ":///stream"

	config := defaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	ch := make(chan *stream.Event, config.BufferLen)
	consumer := &Consumer{
		StreamName: streamName,
		EvtChan:    ch,
		config:     config,
		endpoints:  endpoints,
		target:     target,
	}
	go func() {
		consumer.run()
	}()
	return consumer, nil
}

// SetDNSAddr be used to define the DNS server to use for DNS endpoint type, in format "IP:PORT"
func SetDNSAddr(addr string) {
	mu.Lock()
	defer mu.Unlock()
	authority = addr
}

func grpcTarget(endpointType EndpointType, endpoints []string) string {
	switch endpointType {
	case IPEndpoint:
		// TODO: hacky hack to create a resolver for list of IP addresses
		mu.Lock()
		r, _ := manual.GenerateAndRegisterManualResolver()
		mu.Unlock()

		addresses := make([]resolver.Address, len(endpoints))
		for i := 0; i < len(endpoints); i++ {
			addresses[i] = resolver.Address{Addr: endpoints[i]}
		}
		r.InitialAddrs(addresses)
		return r.Scheme() + ":///stream"
	case DNSEndpoint:
		if len(endpoints) != 1 {
			panic("DNS Grpc endpointType expect only 1 endpoint address, but got " + strconv.Itoa(len(endpoints)))
		}
		return "dns://" + authority + "/" + endpoints[0]
	default:
		panic("unknown Grpc EndpointType " + strconv.Itoa(int(endpointType)))
	}
	return ""
}

func (c *Consumer) run() {
	receivedCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "stream_consumer_received_events",
		Help: "The total number of events received",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(receivedCounter)

	conCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "stream_consumer_connection_attempts",
		Help: "The total number of connections to the stream",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(conCounter)

	conGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "stream_consumer_connected",
		Help: "1 if connected, otherwise 0",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(conGauge)

	delaySummary := prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "stream_consumer_delay_ms",
		Help:       "distribution of delay between when messages are sent to from the consumer and when they are received, in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(delaySummary)

	originDelaySummary := prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "stream_consumer_origin_delay_ms",
		Help:       "distribution of delay between when messages were created by the first producer in the chain of streams, and when they are received, in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(originDelaySummary)

	eventDelaySummary := prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "stream_consumer_event_delay_ms",
		Help:       "distribution of delay between when messages were created and when they are received, in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})
	prometheus.Register(eventDelaySummary)

	var streamClient stream.Stream_StreamClient
	var err error
	var connAttempt uint64

connect:
	for {
		// if it's not the first connection attempt, call onConnectionRetry
		if connAttempt != 0 {
			c.config.onConnectionRetry(c.StreamName, connAttempt)
		}
		conGauge.Set(0)
		Log.Info("trying to connect to stream", zap.String("stream", c.StreamName), zap.Uint64("attempt_number", connAttempt))
		streamClient, err = c.initConn()
		connAttempt++
		conCounter.Inc()

		if err == nil {
			Log.Info("successful connection attempt to stream", zap.String("stream", c.StreamName))
			if c.config.onConnected != nil {
				c.config.onConnected(c.StreamName)
			}
			break
		} else {
			Log.Error("connection attempt to stream failed", zap.String("stream", c.StreamName), zap.Error(err))
		}
	}

	// at this point, the GRPC connection is established with the server
	firstEvent := true
	for {
		streamEvt, err := streamClient.Recv()

		if err != nil {
			Log.Error("stream is unavailable", zap.String("stream", c.StreamName), zap.Error(err))
			if !firstEvent && c.config.onDisconnected != nil {
				c.config.onDisconnected(c.StreamName)
			}
			goto connect
		}


		// if first event received successfully, set the status to connected.
		// we need to do it here because setting up a GRPC connection is not enough, the server can still return us an error
		if firstEvent {
			firstEvent = false
			connAttempt = 0
			conGauge.Set(1)
		}

		Log.Debug("event received", zap.String("stream", c.StreamName))
		receivedCounter.Inc()
		evt := &stream.Event{
			Key:   streamEvt.Key,
			Value: streamEvt.Value,
			Ctx:   stream.MetadataToContext(streamEvt.Metadata),
		}

		nowMs := float64(time.Now().UnixNano()) / 1000000.0

		streamTimestamp := streamEvt.Metadata.StreamTimestamp
		if streamTimestamp > 0 {
			// convert from ns to ms
			delaySummary.Observe(math.Max(0, nowMs-float64(streamTimestamp)/1000000.0))
		}
		eventTimestamp := streamEvt.Metadata.EventTimestamp
		if eventTimestamp > 0 {
			eventDelaySummary.Observe(math.Max(0, nowMs-float64(eventTimestamp)/1000000.0))
		}
		originTimestamp := streamEvt.Metadata.OriginStreamTimestamp
		if originTimestamp > 0 {
			originDelaySummary.Observe(math.Max(0, nowMs-float64(originTimestamp)/1000000.0))
		}
		c.EvtChan <- evt
	}
}

func (c *Consumer) initConn() (stream.Stream_StreamClient, error) {
	mu.RLock()
	//TODO : make grpc.WithInsecure an option
	conn, err := grpc.Dial(c.target, grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name), grpc.WithInsecure())
	mu.RUnlock()
	if err != nil {
		return nil, err
	}
	client := stream.NewStreamClient(conn)
	req := &stream.StreamRequest{Name: c.StreamName}

	var callOpts []grpc.CallOption
	if c.config.UseGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	return client.Stream(context.Background(), req, callOpts...)
}