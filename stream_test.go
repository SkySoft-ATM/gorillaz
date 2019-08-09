package gorillaz

import (
	"bytes"
	"github.com/skysoft-atm/gorillaz/stream"
	"google.golang.org/grpc"
	"net"
	"testing"
	"time"
)

func newGaz() (gaz *Gaz, addr string, shutdown func()) {
	return newGazOnAddr(":0")
}

func newGazOnAddr(conAddr string) (gaz *Gaz, addr string, shutdown func()) {
	g := &Gaz{
		grpcServer: grpc.NewServer(grpc.CustomCodec(&binaryCodec{})),
		streamRegistry: &streamRegistry{
			providers: make(map[string]*StreamProvider),
		},
	}
	l, err := net.Listen("tcp", conAddr)
	if err != nil {
		panic(err)
	}
	stream.RegisterStreamServer(g.grpcServer, g.streamRegistry)
	go g.grpcServer.Serve(l)
	return g, l.Addr().String(), func() {
		g.grpcServer.Stop()
	}
}

func TestStreamLazy(t *testing.T) {
	g, addr, shutdown := newGaz()
	defer shutdown()

	provider, err := g.NewStreamProvider("stream", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot updater provider, %+v", err)
		return
	}

	// as this is a lazy provider, it should wait for a first consumer to send events
	provider.Submit(&stream.Event{Value: []byte("value1")})
	provider.Submit(&stream.Event{Value: []byte("value2")})

	endpoint, err := NewStreamEndpoint(IPEndpoint, []string{addr})
	if err != nil {
		t.Errorf("cannot updater consumer, %+v", err)
		return
	}

	consumer := endpoint.ConsumeStream("stream")

	assertReceived(t, "stream", consumer.EvtChan, &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer.EvtChan, &stream.Event{Value: []byte("value2")})
}

func TestStreamEvents(t *testing.T) {
	g, addr, shutdown := newGaz()
	defer shutdown()

	provider1Stream := "testy"
	provider2Stream := "testoo"

	provider1, err := g.NewStreamProvider(provider1Stream)
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	provider2, err := g.NewStreamProvider(provider2Stream)
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	consumer1 := createConsumer(t, provider1Stream, addr)
	consumer2 := createConsumer(t, provider2Stream, addr)

	evt1 := &stream.Event{
		Key:   []byte("testyKey"),
		Value: []byte("testyValue"),
	}

	evt2 := &stream.Event{
		Key:   []byte("testooKey"),
		Value: []byte("testooValue"),
	}

	// TODO: not great to sleep here, but connected just means we were able to connect the streaming provider
	// it doesn't mean the registration is done on the server side, so we must wait for the registration to be successful
	time.Sleep(time.Second * 1)

	provider1.Submit(evt1)
	provider2.Submit(evt2)

	assertReceived(t, provider1Stream, consumer1.EvtChan, evt1)
	assertReceived(t, provider2Stream, consumer2.EvtChan, evt2)
}

func TestMultipleConsumers(t *testing.T) {
	g, addr, shutdown := newGaz()
	defer shutdown()

	streamName := "testaa"

	provider, err := g.NewStreamProvider(streamName, func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot updater provider, %+v", err)
		return
	}

	consumer1 := createConsumer(t, streamName, addr)
	consumer2 := createConsumer(t, streamName, addr)
	consumer3 := createConsumer(t, streamName, addr)

	// give time to the consumers to be properly subscribed
	time.Sleep(time.Second * 3)

	provider.Submit(&stream.Event{Value: []byte("value1")})
	provider.Submit(&stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer1.EvtChan, &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer1.EvtChan, &stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer2.EvtChan, &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer2.EvtChan, &stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer3.EvtChan, &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer3.EvtChan, &stream.Event{Value: []byte("value2")})
}

func TestProducerReconnect(t *testing.T) {
	g, addr, shutdown := newGaz()
	defer shutdown()

	streamName := "testaa"

	provider, err := g.NewStreamProvider(streamName, func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot updater provider, %+v", err)
		return
	}

	// as this is a lazy provider, it should wait for a first consumer to send events
	provider.Submit(&stream.Event{Value: []byte("value1")})

	consumer := createConsumer(t, streamName, addr)

	assertReceived(t, "stream", consumer.EvtChan, &stream.Event{Value: []byte("value1")})

	// disconnect the provider
	shutdown()

	// wait a bit to be sure the consumer has seen it
	time.Sleep(time.Second)

	g, _, shutdown = newGazOnAddr(addr)
	provider2, err := g.NewStreamProvider(streamName)
	if err != nil {
		t.Errorf("cannot updater provider, %+v", err)
		return
	}

	// let some time for the consumer to figure out the connection is back
	time.Sleep(time.Second * 6)
	provider2.Submit(&stream.Event{Value: []byte("newValue")})

	assertReceived(t, "stream", consumer.EvtChan, &stream.Event{Value: []byte("newValue")})
}

func assertReceived(t *testing.T, streamName string, ch <-chan *stream.Event, expected *stream.Event) {
	select {
	case evt := <-ch:
		if !bytes.Equal(expected.Key, evt.Key) {
			t.Errorf("expected key %v but got key %v", string(expected.Key), string(evt.Key))
		}
		if !bytes.Equal(expected.Value, evt.Value) {
			t.Errorf("expected value %v but got key %v", string(expected.Value), string(evt.Value))
		}
	case <-time.After(time.Second * 5):
		t.Errorf("no event received after 5 sec for stream %s", streamName)
	}
}

func createConsumer(t *testing.T, streamName string, endpoint string) *Consumer {
	connected := make(chan bool, 1)

	opt := func(config *ConsumerConfig) {
		config.OnConnected = func(string) {
			select {
			case connected <- true:
				// ok
			default:
				// nobody is listening, OK too
			}
		}
	}

	streamEndpoint, err := NewStreamEndpoint(IPEndpoint, []string{endpoint})
	if err != nil {
		t.Errorf("cannot create consumer for stream %s,, %+v", streamName, err)
		t.FailNow()
	}

	consumer := streamEndpoint.ConsumeStream(streamName, opt)

	select {
	case <-connected:
		return consumer
	case <-time.After(time.Second * 3):
		t.Errorf("consumer not created after 3 sec for stream %s", streamName)
		t.FailNow()
	}
	return nil
}
