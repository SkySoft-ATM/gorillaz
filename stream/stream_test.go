package stream

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestStreamLazy(t *testing.T) {
	// start a grpc endpoint
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("cannot start a TCP listener, %v", err)
		return
	}
	go manager.server.Serve(l)

	provider, err := NewProvider("stream", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
	})
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	// as this is a lazy provider, it should wait for a first consumer to send events
	provider.Submit(&Event{Value: []byte("value1")})
	provider.Submit(&Event{Value: []byte("value2")})

	consumer, err := NewConsumer("stream", []string{l.Addr().String()})
	if err != nil {
		t.Errorf("cannot start consumer, %+v", err)
		return
	}
	assertReceived(t, "stream", consumer.EvtChan, &Event{Value: []byte("value1")})
	assertReceived(t, "stream", consumer.EvtChan, &Event{Value: []byte("value2")})
}

func TestStreamEvents(t *testing.T) {
	// start
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("cannot start listener, %v", err)
		return
	}
	go manager.server.Serve(l)

	addr := l.Addr()

	provider1Stream := "testy"
	provider2Stream := "testoo"

	provider1, err := NewProvider(provider1Stream)
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	provider2, err := NewProvider(provider2Stream)
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	consumer1 := createConsumer(t, provider1Stream, addr.String())
	consumer2 := createConsumer(t, provider2Stream, addr.String())

	evt1 := &Event{
		Key:   []byte("testyKey"),
		Value: []byte("testyValue"),
	}

	evt2 := &Event{
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

func assertReceived(t *testing.T, streamName string, ch <-chan *Event, expected *Event) {
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
	connected := make(chan bool)

	opt := func(config *ConsumerConfig) {
		config.onConnected = func(string) {
			connected <- true
		}
	}

	consumer, err := NewConsumer(streamName, []string{endpoint}, opt)
	if err != nil {
		t.Errorf("cannot create consumer for stream %s,, %+v", streamName, err)
		t.FailNow()
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
