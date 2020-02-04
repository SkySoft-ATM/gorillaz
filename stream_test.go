package gorillaz

import (
	"bytes"
	"context"
	"fmt"
	prom_client "github.com/prometheus/client_model/go"
	"github.com/skysoft-atm/gorillaz/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"math"
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

func TestConsumerMetrics(t *testing.T) {
	streamName := "creamyCheese"

	gp := New(WithServiceName("cheese_provider"), WithGrpcServerOptions(grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Second, Timeout: time.Second})))
	gp.Run()
	_, err := gp.NewStreamProvider(streamName, "cheesy.type", func(conf *ProviderConfig) {
		conf.LazyBroadcast = false
	})
	if err != nil {
		t.Fatalf("failed to create provider, %+v", err)
	}

	pAddr := fmt.Sprintf("localhost:%d", gp.GrpcPort())

	gc := New(WithServiceName("cheese_consumer"))
	gc.Run()

	createConsumerWithAddr(t, gc, pAddr, streamName)
	assertCounterEquals(t, gc, map[string]string{"stream": streamName}, "stream_consumer_connection_attempts", 1)
	assertCounterEquals(t, gc, map[string]string{"stream": streamName}, "stream_consumer_connection_success", 1)
	assertCounterEquals(t, gc, map[string]string{"stream": streamName}, "stream_consumer_connection_failure", 0)
	assertCounterEquals(t, gc, map[string]string{"stream": streamName}, "stream_consumer_disconnections", 0)
	transientFailure, _ := findMetric(gc, "stream_consumer_connection_status", map[string]string{"stream": streamName, "status": "TRANSIENT_FAILURE"})
	if transientFailure != nil {
		t.Errorf("expected no metric for TRANSIENT_FAILURE for stream before any error happens")
	}

	// cut the gRPC connection
	gp.GrpcServer.Stop()
	time.Sleep(5 * time.Second)
	assertCounterEquals(t, gc, map[string]string{"stream": streamName}, "stream_consumer_disconnections", 1)
	assertCounterMatch(t, gc, map[string]string{"stream": streamName, "status": "TRANSIENT_FAILURE"}, "stream_consumer_connection_status", func(t *testing.T, v float64) {
		if v <= 0 {
			t.Errorf("stream_consumer_connection_status TRANSIENT_FAILURE should be > 0")
		}
	})
	assertCounterMatch(t, gc, map[string]string{"stream": streamName}, "stream_consumer_connection_status_checks", func(t *testing.T, v float64) {
		if v <= 1 {
			t.Errorf("stream_consumer_connection_status_checks should be > 1")
		}
	})
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

func assertCounterEquals(t *testing.T, g *Gaz, labels map[string]string, name string, value float64) {
	match := func(t *testing.T, counterValue float64) {
		if math.Abs(value-counterValue) > 0.01 {
			t.Errorf("expected value %.4f for counter %s for labels %s but got %.4f", value, name, labels, counterValue)
		}
	}
	assertCounterMatch(t, g, labels, name, match)
}

func assertCounterMatch(t *testing.T, g *Gaz, labels map[string]string, name string, match func(*testing.T, float64)) {
	metric, err := findMetric(g, name, labels)
	if err != nil {
		t.Fatalf("failed to find metric %s with labels %+v, %+v", name, labels, err)
	}
	counter := metric.Counter
	if counter == nil {
		t.Fatalf("no counter for metric %s with labels %+v", name, labels)
	}
	match(t, *counter.Value)
}

func findMetric(g *Gaz, metricName string, labelPairs map[string]string) (*prom_client.Metric, error) {
	metricFamilies, err := g.prometheusRegistry.Gather()
	if err != nil {
		return nil, err
	}

	var metricFamily *prom_client.MetricFamily

	for _, mf := range metricFamilies {
		if mf == nil || mf.Name == nil {
			continue
		}
		if *mf.Name == metricName {
			metricFamily = mf
			break
		}
	}
	if metricFamily == nil {
		return nil, fmt.Errorf("metric not found")
	}

	for _, m := range metricFamily.Metric {
		var labelMatchs int
		for k, v := range labelPairs {
			for _, mkv := range m.Label {
				if *mkv.Name == k {
					if *mkv.Value == v {
						labelMatchs++
					}
					break
				}
			}
		}
		if labelMatchs == len(labelPairs) {
			return m, nil
		}
	}

	return nil, fmt.Errorf("no metric found with metric %s and label %+v", metricName, labelPairs)
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

func createConsumerWithAddr(t *testing.T, g *Gaz, endpoint, streamName string) StreamConsumer {
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

	consumer, err := g.ConsumeStream([]string{endpoint}, streamName, opt)
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

func TestDisconnectOnBackpressure(t *testing.T) {
	_, sdOption := NewMockedServiceDiscovery()
	g := New(WithServiceName("test"), sdOption)
	<-g.Run()
	defer g.Shutdown()

	streamName := "TestDisconnectOnBackpressure"

	backPressureHappened := make(chan struct{}, 1)

	provider, err := g.NewStreamProvider(streamName, "dummy.type", func(conf *ProviderConfig) {
		conf.LazyBroadcast = true
		conf.SubscriberInputBufferLen = 10
		conf.OnBackPressure = func(streamName string) {
			backPressureHappened <- struct{}{}
		}
	})
	if err != nil {
		t.Errorf("cannot start provider, %+v", err)
		return
	}

	clientDisconnected := make(chan struct{}, 1)

	consumer, err := g.DiscoverAndConsumeServiceStream("does not matter", streamName, func(cc *ConsumerConfig) {
		cc.OnDisconnected = func(streamName string) {
			clientDisconnected <- struct{}{}
		}
		cc.DisconnectOnBackpressure = true
	})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	backPressureOnProvider := false
	disconnectOnClient := false

	for {

		if backPressureOnProvider && disconnectOnClient {
			return //works as expected
		}

		provider.SubmitNonBlocking(&stream.Event{Value: []byte("a value")})

		select {
		case <-ctx.Done():
			t.Error("Backpressure not seen on time")
		case <-backPressureHappened:
			backPressureOnProvider = true
		case <-clientDisconnected:
			disconnectOnClient = true
		default:
			//send some more
		}
	}
}
