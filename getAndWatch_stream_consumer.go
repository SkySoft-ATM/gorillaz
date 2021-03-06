package gorillaz

import (
	"context"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
)

type GetAndWatchStreamConsumer interface {
	streamConsumer
	EvtChan() chan *stream.GetAndWatchEvent
	Stop() bool //return previous 'stopped' state
}

type registeredGetAndWatchConsumer struct {
	GetAndWatchStreamConsumer
	g *Gaz
}

func (c *registeredGetAndWatchConsumer) Stop() bool {
	wasAlreadyStopped := c.GetAndWatchStreamConsumer.Stop()
	if wasAlreadyStopped {
		Log.Warn("Stop called twice", zap.String("stream name", c.StreamName()))
	} else {
		c.g.deregister(c)
	}
	return wasAlreadyStopped
}

type getAndWatchConsumer struct {
	endpoint   *streamEndpoint
	streamName string
	evtChan    chan *stream.GetAndWatchEvent
	config     *ConsumerConfig
	stopped    *int32
	cMetrics   *consumerMetrics
}

func (c *getAndWatchConsumer) streamEndpoint() *streamEndpoint {
	return c.endpoint
}

func (c *getAndWatchConsumer) StreamName() string {
	return c.streamName
}

func (c *getAndWatchConsumer) EvtChan() chan *stream.GetAndWatchEvent {
	return c.evtChan
}

func (c *getAndWatchConsumer) Stop() bool {
	return atomic.SwapInt32(c.stopped, 1) == 1
}

func (c *getAndWatchConsumer) isStopped() bool {
	return atomic.LoadInt32(c.stopped) == 1
}

func (c *getAndWatchConsumer) metrics() *consumerMetrics {
	return c.cMetrics
}

// Call this method to create a stream consumer
// The service name is resolved via service discovery
// Under the hood we make sure that only 1 subscription is done for a service, even if multiple streams are created on the same service
func (g *Gaz) GetAndWatchStream(service, stream string, opts ...ConsumerConfigOpt) (GetAndWatchStreamConsumer, error) {
	return g.createGetAndWatchConsumer([]string{SdPrefix + service}, stream, opts...)
}

func (g *Gaz) createGetAndWatchConsumer(endpoints []string, streamName string, opts ...ConsumerConfigOpt) (GetAndWatchStreamConsumer, error) {
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
	sc := e.getAndWatch(streamName, opts...)
	rc := registeredGetAndWatchConsumer{g: r.g, GetAndWatchStreamConsumer: sc}
	consumers := r.endpointConsumers[e]
	if consumers == nil {
		consumers = make(map[StoppableStream]struct{})
		r.endpointConsumers[e] = consumers
	}
	consumers[&rc] = struct{}{}
	return &rc, nil
}

func (se *streamEndpoint) getAndWatch(streamName string, opts ...ConsumerConfigOpt) GetAndWatchStreamConsumer {
	config := defaultConsumerConfig()
	for _, opt := range opts {
		opt(config)
	}

	ch := make(chan *stream.GetAndWatchEvent, config.BufferLen)
	c := &getAndWatchConsumer{
		endpoint:   se,
		streamName: streamName,
		evtChan:    ch,
		config:     config,
		stopped:    new(int32),
		cMetrics:   consumerMonitoring(se.g, streamName, se.endpoints),
	}

	go func() {
		c.reconnectGetAndWatchWhileNotStopped()
		Log.Info("Stream closed", zap.String("stream", c.streamName))
		close(c.evtChan)
	}()
	return c
}

func (c *getAndWatchConsumer) reconnectGetAndWatchWhileNotStopped() {
	for c.endpoint.conn.GetState() != connectivity.Shutdown && !c.isStopped() {
		waitTillConnReadyOrShutdown(c)
		if c.endpoint.conn.GetState() == connectivity.Shutdown {
			break
		}
		retry := c.readGetAndWatchStream()
		if !retry {
			return
		}
	}
}

func (c *getAndWatchConsumer) readGetAndWatchStream() (retry bool) {
	client := stream.NewStreamClient(c.endpoint.conn)
	req := &stream.GetAndWatchRequest{
		Name:                     c.streamName,
		RequesterName:            c.endpoint.g.ServiceName,
		ExpectHello:              true,
		DisconnectOnBackpressure: c.config.DisconnectOnBackpressure,
	}

	var callOpts []grpc.CallOption
	if c.config.UseGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	callOpts = append(callOpts, grpc.CallContentSubtype(StreamEncoding))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st, err := client.GetAndWatch(ctx, req, callOpts...)
	if err != nil {
		Log.Warn("Error while creating stream", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		return true
	}

	//without this hack we do not know if the stream is really connected
	mds, err := st.Header()
	if err == nil && mds != nil {

		if c.config.OnConnected != nil {
			c.config.OnConnected(c.streamName)
		}
		Log.Debug("Stream connected", zap.String("streamName", c.streamName), zap.String("target", c.endpoint.target))

		// at this point, the GRPC connection is established with the server
		for !c.isStopped() {
			c.cMetrics.conGauge.Set(1)
			gwEvt, err := st.Recv()

			if err != nil {
				if err == io.EOF {
					Log.Info("received EOF, stream closed", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
					c.cMetrics.conGauge.Set(0)
					return false //standard error for closed stream
				}
				Log.Warn("received error on stream", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
				if e, ok := status.FromError(err); ok {
					switch e.Code() {
					case codes.PermissionDenied, codes.ResourceExhausted, codes.Unavailable,
						codes.Unimplemented, codes.NotFound, codes.Unauthenticated, codes.Unknown:
						time.Sleep(5 * time.Second)
					}
				}
				break
			}

			if gwEvt == nil {
				Log.Warn("received a nil stream event", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
				return true
			}
			if gwEvt.Metadata == nil {
				Log.Debug("received a nil stream.Metadata, creating an empty metadata", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
				gwEvt.Metadata = &stream.Metadata{
					KeyValue: make(map[string]string),
				}
			}
			Log.Debug("event received", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target))
			monitorDelays(c, gwEvt)

			c.evtChan <- gwEvt
		}
	} else {
		if mds == nil {
			Log.Warn("Stream created but not connected, no header received", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		} else {
			Log.Warn("Stream created but not connected", zap.String("stream", c.streamName), zap.String("target", c.endpoint.target), zap.Error(err))
		}
		time.Sleep(5 * time.Second)
	}
	c.cMetrics.conGauge.Set(0)
	if c.config.OnDisconnected != nil {
		c.config.OnDisconnected(c.streamName)
	}
	return true
}
