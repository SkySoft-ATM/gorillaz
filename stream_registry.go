package gorillaz

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"net"
	"strconv"
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
	sendLoop(strm grpc.ServerStream, peer Peer)
	streamType() stream.StreamType
	sendHelloMessage(strm grpc.ServerStream, peer Peer) error
}

type streamRegistry struct {
	sync.RWMutex
	g          *Gaz
	providers  map[string]provider
	serviceIds map[string]RegistrationHandle // for each stream a service is registered in the service discovery
}

func NewStreamRegistry(g *Gaz) *streamRegistry {
	sr := &streamRegistry{
		g:          g,
		providers:  make(map[string]provider),
		serviceIds: make(map[string]RegistrationHandle),
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
		err = sr.g.streamDefinitions.Submit(&se)
		if err != nil {
			panic(err)
		}
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
	GetExpectHello() bool
}

// Stream implements streaming.proto Stream.
// should not be called by the client
func (sr *streamRegistry) Stream(req *stream.StreamRequest, strm stream.Stream_StreamServer) error {
	return sr.publishOnStream(req, strm)
}

func (sr *streamRegistry) publishOnStream(np StreamRequest, strm grpc.ServerStream) error {
	peer := getPeer(strm, np)
	streamName := np.GetName()
	requester := np.GetRequesterName()
	Log.Info("new stream consumer", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("requester", requester))
	sr.RLock()
	provider, ok := sr.providers[streamName]
	sr.RUnlock()
	if !ok {
		Log.Warn("unknown stream", zap.String("stream", streamName), zap.String("peer", peer.address), zap.String("requester", requester))
		return fmt.Errorf("unknown stream %s", streamName)
	}
	// we send some metadata for backward compatibility, it was previously used on the client side to check if the stream connection is really established
	header := metadata.Pairs("name", streamName, "expectHello", strconv.FormatBool(np.GetExpectHello()))
	err := strm.SendHeader(header)
	if err != nil {
		Log.Error("client might be disconnected %s", zap.Error(err), zap.String("peer", peer.address), zap.String("requester", requester))
		return err
	}
	if np.GetExpectHello() {
		err := provider.sendHelloMessage(strm, peer)
		if err != nil {
			return err
		}
	}
	provider.sendLoop(strm, peer)
	return nil
}

func getPeer(strm grpc.ServerStream, np StreamRequest) Peer {
	return Peer{GetGrpcClientAddress(strm.Context()), np.GetRequesterName()}
}

func (sr *streamRegistry) GetAndWatch(req *stream.GetAndWatchRequest, strm stream.Stream_GetAndWatchServer) error {
	return sr.publishOnStream(req, strm)
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

func (g *Gaz) DiscoverStreamDefinitions(serviceName string) (GetAndWatchStreamConsumer, error) {
	return g.GetAndWatchStream(serviceName, streamDefinitions)
}
