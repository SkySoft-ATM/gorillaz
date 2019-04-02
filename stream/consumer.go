package stream

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	gaz "github.com/skysoft-atm/gorillaz"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"math"
	"strings"
	"sync"
	"time"
)

var mu sync.RWMutex

type ConsumerConfig struct {
	BufferLen         int                                     // BufferLen is the size of the channel of the consumer
	onConnectionRetry func(streamName string, retryNb uint64) // onConnectionRetry is called before trying to reconnect to a stream provider
	onConnected       func(streamName string)
	onDisconnected    func(streamName string)
}

type Consumer struct {
	StreamName string
	EvtChan    chan *Event
	target     string
	endpoints  []string
	config     *ConsumerConfig
}

func defaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BufferLen: 256,
		onConnectionRetry: func(streamName string, retryNb uint64) {
			wait := time.Second * 0
			switch retryNb {
			case 0:
				// just try to connect directly on the first attempt
				break
			case 1:
				wait = time.Second
			case 2:
				wait = time.Second * 2
			case 3:
				wait = time.Second * 3
			default:
				wait = time.Second * 5
			}
			if wait > 0 {
				gaz.Log.Info("waiting before making another connection attempt", zap.String("streamName", streamName), zap.Int("wait_sec", int(wait.Seconds())))
				time.Sleep(wait)
			}
			gaz.Log.Info("trying to connect to stream", zap.String("stream", streamName), zap.Uint64("retry_nb", retryNb))
		},
	}
}

type ConsumerConfigOpt func(*ConsumerConfig)

func NewConsumer(streamName string, endpoints []string, opts ...ConsumerConfigOpt) (*Consumer, error) {
	// TODO: hacky hack to create a resolver to use with round robin
	mu.Lock()
	r, _ := manual.GenerateAndRegisterManualResolver()
	mu.Unlock()

	addresses := make([]resolver.Address, len(endpoints))
	for i := 0; i < len(endpoints); i++ {
		addresses[i] = resolver.Address{Addr: endpoints[i]}
	}
	r.InitialAddrs(addresses)
	target := r.Scheme() + ":///fake"

	config := defaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	ch := make(chan *Event, config.BufferLen)
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

func (c *Consumer) run() {
	receivedCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_consumer_received_events",
		Help: "The total number of events received",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})

	conCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "stream_consumer_connection_attempts",
		Help: "The total number of connections to the stream",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})

	conGauge := promauto.NewGauge(prometheus.GaugeOpts{
		Name: "stream_consumer_connected",
		Help: "Set to 1 if connected, otherwise 0",
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})

	delaySummary := promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "stream_consumer_delay_ms",
		Help:       "The distribution of delay between when messages are sent to from the consumer and when they are received, in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		ConstLabels: prometheus.Labels{
			"stream":    c.StreamName,
			"endpoints": strings.Join(c.endpoints, ","),
		},
	})

	var streamClient Stream_StreamClient
	var err error
	var connRetry uint64
connect:
	conGauge.Set(0)
	for {
		conCounter.Inc()
		streamClient, err = c.initConn()
		if err == nil {
			connRetry = 0
			conGauge.Set(1)
			gaz.Log.Info("successful connection attempt to stream", zap.String("stream", c.StreamName))
			break
		} else {
			gaz.Log.Error("connection attempt to stream failed", zap.String("stream", c.StreamName), zap.Error(err))
			c.config.onConnectionRetry(c.StreamName, connRetry)
			connRetry++
		}
	}
	if c.config.onConnected != nil {
		c.config.onConnected(c.StreamName)
	}
	for {
		streamEvt, err := streamClient.Recv()
		if err != nil {
			conGauge.Set(0)
			gaz.Log.Error("stream is unavailable", zap.String("stream", c.StreamName), zap.Error(err))
			if c.config.onDisconnected != nil {
				c.config.onDisconnected(c.StreamName)
			}
			goto connect
		}
		gaz.Log.Debug("event received", zap.String("stream", c.StreamName))
		receivedCounter.Inc()
		evt := &Event{
			Key:   streamEvt.Key,
			Value: streamEvt.Value,
			Ctx:   metadataToContext(streamEvt.Metadata),
		}
		streamTimestamp := StreamTimestamp(evt)
		if streamTimestamp > 0 {
			receptTime := time.Now()
			// convert from ns to ms
			delaySummary.Observe(math.Max(0, float64(receptTime.UnixNano())/1000000.0-float64(streamTimestamp)/1000000.0))
		}
		c.EvtChan <- evt
	}
}

func (c *Consumer) initConn() (Stream_StreamClient, error) {
	mu.RLock()
	//TODO : make grpc.WithInsecure an option
	conn, err := grpc.Dial(c.target, grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	mu.RUnlock()
	if err != nil {
		return nil, err
	}
	client := NewStreamClient(conn)
	req := &StreamRequest{Name: c.StreamName}
	// TODO: do we want to pass a context here?
	return client.Stream(context.Background(), req)
}
