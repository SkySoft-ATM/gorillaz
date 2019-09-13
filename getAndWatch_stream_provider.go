package gorillaz

import (
	"encoding/base64"
	"github.com/golang/protobuf/proto"
	"github.com/skysoft-atm/gorillaz/mux"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"time"
)

type GetAndWatchStreamProvider struct {
	streamDef   *StreamDefinition
	config      *GetAndWatchConfig
	broadcaster *mux.StateBroadcaster
	metrics     providerMetricsHolder
	gaz         *Gaz
}

func (p *GetAndWatchStreamProvider) streamType() stream.StreamType {
	return stream.StreamType_GET_AND_WATCH
}

type GetAndWatchConfigOpt func(p *GetAndWatchConfig)

// ProviderConfig is the configuration that will be applied for the stream StreamProvider
type GetAndWatchConfig struct {
	InputBufferLen           int                     // InputBufferLen is the size of the input channel (default: 256)
	SubscriberInputBufferLen int                     // SubscriberInputBufferLen is the size of the channel used to forward events to each client. (default: 256)
	OnBackPressure           func(streamName string) // OnBackPressure is the function called when a customer cannot consume fast enough and event are dropped. (default: log)
	Ttl                      time.Duration
}

func defaultGetAndWatchConfig() *GetAndWatchConfig {
	return &GetAndWatchConfig{
		InputBufferLen:           256,
		SubscriberInputBufferLen: 256,
		OnBackPressure: func(streamName string) {
			Log.Warn("backpressure applied, an event won't be delivered because it can't consume fast enough", zap.String("stream", streamName))
		},
		Ttl: 0,
	}
}

// NewStreamProvider returns a new provider ready to be used.
// only one instance of provider should be created for a given streamName
func (g *Gaz) NewGetAndWatchStreamProvider(streamName, dataType string, opts ...GetAndWatchConfigOpt) (*GetAndWatchStreamProvider, error) {
	Log.Info("creating stream", zap.String("stream", streamName))

	config := defaultGetAndWatchConfig()
	for _, opt := range opts {
		opt(config)
	}

	broadcaster, err := mux.NewNonBlockingStateBroadcaster(config.InputBufferLen, config.Ttl)
	if err != nil {
		Log.Error("could not create state broadcaster", zap.Error(err))
		return nil, err
	}

	p := &GetAndWatchStreamProvider{
		streamDef:   &StreamDefinition{Name: streamName, DataType: dataType},
		config:      config,
		broadcaster: broadcaster,
		metrics:     pMetricHolder(g, streamName),
		gaz:         g,
	}
	g.streamRegistry.register(p)
	return p, nil
}

// Submit pushes the event to all subscribers and stores it by its key for new subscribers appearing on the stream
func (p *GetAndWatchStreamProvider) Submit(evt *stream.Event) error {
	p.metrics.sentCounter.Inc()
	p.metrics.lastEventTimestamp.SetToCurrentTime()

	return p.broadcaster.Submit(base64.StdEncoding.EncodeToString(evt.Key), evt)
}

func (p *GetAndWatchStreamProvider) Delete(key []byte) {
	p.broadcaster.Delete(base64.StdEncoding.EncodeToString(key))
}

func (p *GetAndWatchStreamProvider) sendLoop(strm grpc.ServerStream, peer Peer) {
	streamName := p.streamDef.Name
	p.metrics.clientCounter.Inc()
	defer p.metrics.clientCounter.Dec()
	broadcaster := p.broadcaster
	streamCh := make(chan *mux.StateUpdate, p.config.SubscriberInputBufferLen)
	broadcaster.Register(streamCh, func(config *mux.ConsumerConfig) error {
		config.OnBackpressure(func(interface{}) {
			p.config.OnBackPressure(streamName)
			p.metrics.backPressureCounter.Inc()
		})
		return nil
	})
	defer broadcaster.Unregister(streamCh)

forloop:
	for {
		select {
		case su, ok := <-streamCh:
			if !ok {
				break forloop //channel closed
			}
			gwe := stream.GetAndWatchEvent{
				Metadata: &stream.Metadata{
					KeyValue: make(map[string]string),
				},
			}

			if su.UpdateType == mux.Delete {
				key := su.Value.([]byte)
				gwe.Key = key
				gwe.EventType = stream.EventType_DELETE
			} else {
				se := su.Value.(*stream.Event)
				if su.UpdateType == mux.Update {
					gwe.EventType = stream.EventType_UPDATE
				} else {
					gwe.EventType = stream.EventType_INITIAL_STATE
				}
				gwe.Key = se.Key
				gwe.Value = se.Value
				stream.ContextToMetadata(se.Ctx, gwe.Metadata)
			}
			evt, err := proto.Marshal(&gwe)
			if err != nil {
				Log.Error("Error while marshalling GetAndWatchEvent", zap.Error(err))
			}
			if err := strm.(grpc.ServerStream).SendMsg(evt); err != nil {
				Log.Info("consumer disconnected", zap.Error(err), zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("peer service", peer.serviceName))
				break forloop
			}
		case _ = <-strm.Context().Done():
			Log.Info("consumer disconnected", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("peer service", peer.serviceName))
			break forloop

		}
	}

}

func (p *GetAndWatchStreamProvider) CloseStream() error {
	return p.gaz.closeStream(p)
}

func (p *GetAndWatchStreamProvider) close() {
	p.broadcaster.Close()
}

func (p *GetAndWatchStreamProvider) streamDefinition() *StreamDefinition {
	return p.streamDef
}
