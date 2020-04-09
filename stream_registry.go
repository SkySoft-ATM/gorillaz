package gorillaz

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"net"
	"sync"
)

const (
	streamDefinitions = "streamDefinitions"
	StreamProviderTag = "streamProvider"
)

type StreamDefinition struct {
	Name     string
	DataType string
}

type provider interface {
	close()
	streamDefinition() *StreamDefinition
	sendLoop(strm grpc.ServerStream, peer Peer, opts sendLoopOpts) error
	streamType() stream.StreamType
	sendHelloMessage(strm grpc.ServerStream, peer Peer) error
}

type sendLoopOpts struct {
	disconnectOnBackpressure bool
}

type streamRegistry struct {
	sync.RWMutex
	g         *Gaz
	providers map[string]provider
}

func newStreamRegistry(g *Gaz) *streamRegistry {
	sr := &streamRegistry{
		g:         g,
		providers: make(map[string]provider),
	}
	return sr
}

func (sr *streamRegistry) find(streamName string) (provider, bool) {
	sr.RLock()
	p, ok := sr.providers[streamName]
	sr.RUnlock()
	return p, ok
}

func (g *Gaz) closeStream(p provider) error {
	streamName := p.streamDefinition().Name
	Log.Info("closing stream", zap.String("stream", streamName))
	prov, ok := g.streamRegistry.find(streamName)
	if !ok {
		return fmt.Errorf("cannot find stream " + streamName)
	}
	g.streamRegistry.unregister(streamName)
	prov.close()
	return nil
}

func (sr *streamRegistry) register(p provider) {
	streamName := p.streamDefinition().Name
	sr.Lock()
	defer sr.Unlock()
	if _, found := sr.providers[streamName]; found {
		panic("cannot register 2 providers with the same streamName: " + streamName)
	}
	sr.providers[streamName] = p
	sd := stream.StreamDefinition{
		Name:       streamName,
		DataType:   p.streamDefinition().DataType,
		StreamType: p.streamType(),
	}

	bytes, err := proto.Marshal(&sd)
	if err != nil {
		panic(err)
	}
	se := stream.Event{Ctx: nil, Key: []byte(streamName), Value: bytes}

	if sr.g.streamDefinitions != nil {
		sr.g.streamDefinitions.Submit(&se)
	}
}

func (sr *streamRegistry) unregister(streamName string) {
	sr.Lock()
	defer sr.Unlock()
	_, ok := sr.providers[streamName]
	if ok {
		delete(sr.providers, streamName)
		sr.g.streamDefinitions.Delete([]byte(streamName))
	}
}

type StreamRequest interface {
	GetName() string
	GetRequesterName() string
	GetDisconnectOnBackpressure() bool
}

// Stream implements streaming.proto Stream.
// should not be called by the client
func (sr *streamRegistry) Stream(req *stream.StreamRequest, strm stream.Stream_StreamServer) error {
	peer := getPeer(strm, req)
	streamName := req.GetName()
	requester := req.GetRequesterName()

	opts := sendLoopOpts{
		disconnectOnBackpressure: req.GetDisconnectOnBackpressure(),
	}

	Log.Info("new stream consumer", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("requester", requester))
	sr.RLock()
	provider, ok := sr.providers[streamName]
	sr.RUnlock()
	if !ok {
		Log.Warn("unknown stream", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("requester", requester))
		return fmt.Errorf("unknown stream %s", streamName)
	}
	err := provider.sendHelloMessage(strm, peer)
	if err != nil {
		return err
	}
	return provider.sendLoop(strm, peer, opts)
}

func getPeer(strm grpc.ServerStream, np StreamRequest) Peer {
	return Peer{GetGrpcClientAddress(strm.Context()), np.GetRequesterName()}
}

type Peer struct {
	address     string
	serviceName string
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

func (g *Gaz) DiscoverStreamDefinitions(serviceName string) (StreamConsumer, error) {
	return g.DiscoverAndConsumeServiceStream(serviceName, streamDefinitions)
}
