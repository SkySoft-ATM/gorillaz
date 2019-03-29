package stream

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

var mu sync.RWMutex

func NewConsumer(streamName string, endpoints ...string) (chan *Event, error){
	// TODO: hacky hack to create a resolver to use with round robin
	mu.Lock()
	r,_ := manual.GenerateAndRegisterManualResolver()
	mu.Unlock()

	addresses := make([]resolver.Address, len(endpoints))
	for i:=0;i<len(endpoints);i++{
		addresses[i] = resolver.Address{Addr: endpoints[i]}
	}
	r.InitialAddrs(addresses)

	mu.RLock()
	conn, err := grpc.Dial(r.Scheme()+":///fake", grpc.WithInsecure(), grpc.WithBalancerName(roundrobin.Name))
	mu.RUnlock()
	if err != nil {
		return nil, err
	}
	c := NewStreamClient(conn)
	req := &StreamRequest{Name:streamName}
	stream, err := c.Stream(context.TODO(), req)
	if err != nil {
		return nil, err
	}
	ch := make(chan *Event, 256)

	receivedCounter := promauto.NewCounter(prometheus.CounterOpts{
		Name: "received_events",
		Help: "The total number of events received",
		ConstLabels:prometheus.Labels{
			"stream": streamName,
			"endpoints": strings.Join(endpoints,","),
		},
	})

	delaySummary := promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "streaming_delay_ms",
		Help:       "The distribution of delay between when messages are sent to from the consumer and when they are received, in milliseconds",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		ConstLabels:prometheus.Labels{
			"stream":streamName,
			"endpoints": strings.Join(endpoints,","),
		},
	})

	go func() {
		for {
			streamEvt, err := stream.Recv()
			if err != nil {
				log.Printf("ERROR: stream %s is unavailable, %v\n", streamName, err)
				close(ch)
				conn.Close()
				return
			}
			receivedCounter.Inc()
			receptTime := time.Now()
			delaySummary.Observe(math.Max(0,float64(receptTime.UnixNano())/1000000.0-float64(streamEvt.Stream_Timestamp_Ns)/1000000.0))
			ch <- &Event{
				Key: streamEvt.Key,
				Value: streamEvt.Value,
				StreamTimestamp: streamEvt.Stream_Timestamp_Ns,
			}
		}
	}()
	return ch, nil
}
