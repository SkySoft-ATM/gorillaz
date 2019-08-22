package gorillaz

import (
	"bytes"
	"fmt"
	"github.com/skysoft-atm/gorillaz/stream"
	"testing"
	"time"
)

func TestFullStreamName(t *testing.T) {
	const full = "toto.tutu.stream"
	const srv = "toto.tutu"
	const str = "stream"
	serv, stream := ParseStreamName(full)
	assertEquals(t, serv, srv, "as service name")
	assertEquals(t, stream, str, "as stream")

	name := GetFullStreamName(srv, str)
	assertEquals(t, name, full, "as full stream name")
}

func assertEquals(t *testing.T, got, expected, comment string) {
	if got != expected {
		t.Error(fmt.Sprintf("%s Got: %s Expected: %s", comment, got, expected))
	}
}

func TestStreamLazy(t *testing.T) {
	g := New(WithServiceName("test"), WithMockedServiceDiscovery())
	defer g.Shutdown()
	<-g.Run()

	provider, err := g.NewStreamProvider("stream", "dummy.type", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	// as this is a lazy provider, it should wait for a first consumer to send events
	provider.Submit(&stream.Event{Value: []byte("value1")})
	provider.Submit(&stream.Event{Value: []byte("value2")})

	consumer, err := g.DiscoverAndConsumeServiceStream("does not mater", "stream")
	if err != nil {
		t.Fatal(err)
	}

	assertReceived(t, "stream", consumer.EvtChan(), &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer.EvtChan(), &stream.Event{Value: []byte("value2")})
}

func TestStreamEvents(t *testing.T) {
	g := New(WithServiceName("test"), WithMockedServiceDiscovery())
	defer g.Shutdown()
	<-g.Run()

	provider1Stream := "testy"
	provider2Stream := "testoo"

	provider1, err := g.NewStreamProvider(provider1Stream, "dummy.type")
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	provider2, err := g.NewStreamProvider(provider2Stream, "dummy.type")
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	consumer1 := createConsumer(t, g, provider1Stream)
	consumer2 := createConsumer(t, g, provider2Stream)

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

	assertReceived(t, provider1Stream, consumer1.EvtChan(), evt1)
	assertReceived(t, provider2Stream, consumer2.EvtChan(), evt2)
}

func TestMultipleConsumers(t *testing.T) {
	g := New(WithServiceName("test"), WithMockedServiceDiscovery())
	defer g.Shutdown()
	<-g.Run()

	streamName := "testaa"

	provider, err := g.NewStreamProvider(streamName, "dummy.type", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	consumer1 := createConsumer(t, g, streamName)
	consumer2 := createConsumer(t, g, streamName)
	consumer3 := createConsumer(t, g, streamName)

	// give time to the consumers to be properly subscribed
	time.Sleep(time.Second * 3)

	provider.Submit(&stream.Event{Value: []byte("value1")})
	provider.Submit(&stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer1.EvtChan(), &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer1.EvtChan(), &stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer2.EvtChan(), &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer2.EvtChan(), &stream.Event{Value: []byte("value2")})

	assertReceived(t, "stream", consumer3.EvtChan(), &stream.Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer3.EvtChan(), &stream.Event{Value: []byte("value2")})
}

func TestProducerReconnect(t *testing.T) {
	mock, sdOption := NewMockedServiceDiscovery()
	g := New(WithServiceName("test"), sdOption)
	<-g.Run()

	streamName := "testaa"

	provider, err := g.NewStreamProvider(streamName, "dummy.type", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	// as this is a lazy provider, it should wait for a first consumer to send events
	provider.Submit(&stream.Event{Value: []byte("value1")})

	consumer, err := g.DiscoverAndConsumeServiceStream("does not matter", streamName)
	if err != nil {
		t.Fatal(err)
	}

	assertReceived(t, "stream", consumer.EvtChan(), &stream.Event{Value: []byte("value1")})

	// disconnect the provider
	g.Shutdown()

	// wait a bit to be sure the consumer has seen it
	time.Sleep(time.Second)

	g = New(WithServiceName("test"))
	<-g.Run()
	mock.UpdateGaz(g)
	defer g.Shutdown()
	provider2, err := g.NewStreamProvider(streamName, "dummy.type")
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	// let some time for the consumer to figure out the connection is back
	time.Sleep(time.Second * 6)
	provider2.Submit(&stream.Event{Value: []byte("newValue")})

	assertReceived(t, "stream", consumer.EvtChan(), &stream.Event{Value: []byte("newValue")})
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

func createConsumer(t *testing.T, g *Gaz, streamName string) StreamConsumer {
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

	consumer, err := g.DiscoverAndConsumeServiceStream("does not matter", streamName, opt)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-connected:
		return consumer
	case <-time.After(time.Second * 3):
		t.Errorf("consumer not created after 3 sec for stream %s", streamName)
		t.FailNow()
	}
	return nil
}
