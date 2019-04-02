package stream

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestStreamEvents(t *testing.T){
	// start
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Errorf("cannot start listener, %v", err)
		return
	}
	go manager.server.Serve(l)

	addr := l.Addr()

	providerTesty, err := NewProvider("testy")
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	providerTestoo, err := NewProvider("testoo")
	if err != nil {
		t.Errorf("cannot register provider, %+v", err)
		return
	}

	conTesty,err := NewConsumer("testy", []string{addr.String()})
	if err != nil {
		t.Errorf("cannot create consumer, %+v", err)
		return
	}

	conTestoo,err := NewConsumer("testoo", []string{addr.String()})
	if err != nil {
		t.Errorf("cannot create consumer, %+v", err)
		return
	}

	evtTesty := &Event{
		Key: []byte("testyKey"),
		Value: []byte("testyValue"),
	}

	evtTestoo := &Event{
		Key: []byte("testooKey"),
		Value: []byte("testooValue"),
	}

	// wait a bit to be sure the consumer is properly connected
	time.Sleep(time.Second*2)

	providerTesty.Submit(evtTesty)
	providerTestoo.Submit(evtTestoo)

	assertReceived(t, conTesty, evtTesty)
	assertReceived(t, conTestoo, evtTestoo)
}

func assertReceived(t *testing.T, ch chan *Event, expected *Event){
	select {
	case evt := <- ch:
		if !bytes.Equal(expected.Key, evt.Key) {
			t.Errorf("expected key %v but got key %v", string(expected.Key), string(evt.Key))
		}
		if !bytes.Equal(expected.Value, evt.Value) {
			t.Errorf("expected value %v but got key %v", string(expected.Value), string(evt.Value))
		}
	case <- time.After(time.Second*5):
		t.Errorf("no event received after 3 sec")
	}
}