package gorillaz

import (
	"context"
	"github.com/pkg/errors"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"io"
	"strings"
	"sync/atomic"
	"time"
)

type GetAndWatchStreamConsumer interface {
	StreamName() string
	EvtChan() chan *stream.GetAndWatchEvent
	Stop() bool //return previous 'stopped' state
	streamEndpoint() *streamEndpoint
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
	}

	var monitoringHolder = consumerMonitoring(se.g, streamName, se.endpoints)

	go func() {
		se.reconnectGetAndWatchWhileNotStopped(c, streamName, config, monitoringHolder)
		Log.Info("Stream closed", zap.String("stream", c.streamName))
		close(c.evtChan)
	}()
	return c
}

func (se *streamEndpoint) reconnectGetAndWatchWhileNotStopped(c *getAndWatchConsumer, streamName string, config *ConsumerConfig, monitoringHolder consumerMonitoringHolder) {
	for se.conn.GetState() != connectivity.Shutdown && !c.isStopped() {
		waitTillReadyOrShutdown(streamName, se)
		if se.conn.GetState() == connectivity.Shutdown {
			break
		}

		client := stream.NewStreamClient(se.conn)
		req := &stream.GetAndWatchRequest{Name: streamName, RequesterName: se.g.ServiceName}

		var callOpts []grpc.CallOption
		if config.UseGzip {
			callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
		}
		ctx, cancel := context.WithCancel(context.Background())
		st, err := client.GetAndWatch(ctx, req, callOpts...)
		if err != nil {
			cancel()
			Log.Warn("Error while creating stream", zap.String("stream", streamName), zap.String("target", se.target), zap.Error(err))
			continue
		}

		//without this hack we do not know if the stream is really connected
		mds, err := st.Header()
		if err == nil && mds != nil {

			if config.OnConnected != nil {
				config.OnConnected(streamName)
			}
			Log.Debug("Stream connected", zap.String("streamName", streamName), zap.String("target", se.target))

			// at this point, the GRPC connection is established with the server
			for !c.isStopped() {
				monitoringHolder.conGauge.Set(1)
				gwEvt, err := st.Recv()

				if err != nil {
					if err == io.EOF {
						Log.Debug("Stream closed", zap.String("stream", streamName), zap.String("target", se.target))
						monitoringHolder.conGauge.Set(0)
						return //standard error for closed stream
					}
					Log.Warn("received error on stream", zap.String("stream", c.streamName), zap.String("target", se.target), zap.Error(err))
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
					Log.Warn("received a nil stream event", zap.String("stream", streamName), zap.String("target", se.target))
					continue
				}
				if gwEvt.Metadata == nil {
					Log.Debug("received a nil stream.Metadata, creating an empty metadata", zap.String("stream", streamName), zap.String("target", se.target))
					gwEvt.Metadata = &stream.Metadata{
						KeyValue: make(map[string]string),
					}
				}
				Log.Debug("event received", zap.String("stream", streamName), zap.String("target", se.target))
				monitorDelays(monitoringHolder, gwEvt)

				c.evtChan <- gwEvt
			}
		} else {
			if mds == nil {
				Log.Warn("Stream created but not connected, no header received", zap.String("stream", streamName), zap.String("target", se.target), zap.Error(err))
			} else {
				Log.Warn("Stream created but not connected", zap.String("stream", streamName), zap.String("target", se.target), zap.Error(err))
			}
			time.Sleep(5 * time.Second)
		}
		monitoringHolder.conGauge.Set(0)
		if config.OnDisconnected != nil {
			config.OnDisconnected(streamName)
		}
		cancel()
	}
}
