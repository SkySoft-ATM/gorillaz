package mux

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func backpressureForConsumer(consumerName string, consumer chan string) func(value interface{}) {
	return func(value interface{}) {
		fmt.Println("on back pressure " + consumerName)
		consumer <- consumerName
	}
}

func backpressureOptionForConsumer(consumerName string, consumer chan string) func(config *ConsumerConfig) error {
	return func(config *ConsumerConfig) error {
		config.OnBackpressure(backpressureForConsumer(consumerName, consumer))
		return nil
	}
}

func TestBackpressureOnConsumer(t *testing.T) {

	toSend := 20

	b := NewNonBlockingBroadcaster(toSend)

	fastStarted := make(chan bool)
	fastConsumerChan := make(chan interface{})
	go func() {
		fastStarted <- true
		for range fastConsumerChan {
			// poll as fast as possible
		}
	}()

	// wait for fast consumer started
	<-fastStarted

	slowStarted := make(chan bool)
	slowConsumerChan := make(chan interface{})
	go func() {
		// only consume 5 messages and stop working to simulate slow consumption after 5 messages
		slowStarted <- true
		for i := 0; i < 5; i++ {
			<-slowConsumerChan
		}
	}()

	// wait for slow consumer to have started
	<-slowStarted

	var backPressureChan = make(chan string, 2*toSend+1)

	b.Register(fastConsumerChan, backpressureOptionForConsumer("fast", backPressureChan))
	b.Register(slowConsumerChan, backpressureOptionForConsumer("slow", backPressureChan))

	for i := 0; i < toSend; i++ {
		b.SubmitBlocking(i)
		// make sure the fast consumer can actually consume it fast enough
		time.Sleep(time.Millisecond * 20)
	}
	b.Close()

	time.Sleep(time.Second)
	close(backPressureChan)

	backPressureCount := 0
	for backPressMsg := range backPressureChan {
		backPressureCount++
		if backPressMsg != "slow" {
			t.Errorf("expected only slow consumer to have backpressure, but it also applies to %s", backPressMsg)
		}
	}
	if backPressureCount != toSend-5 {
		t.Errorf("expected %d backpressure events but got %d", toSend-5, backPressureCount)
	}
}

func TestBackpressureOnProducer(t *testing.T) {
	b := NewNonBlockingBroadcaster(0, LazyBroadcast)
	var sent = make(chan bool, 1)
	go func() {
		b.SubmitBlocking("someValue")
		sent <- true
	}()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-sent:
		t.Error("Call did not block")
	case <-timeout:
		t.Log("Call correctly blocked")
	}
}

func TestProducerDropsMessageOnBackpressure(t *testing.T) {
	b := NewNonBlockingBroadcaster(0, LazyBroadcast)
	var sent = make(chan bool, 1)
	go func() {
		err := b.SubmitNonBlocking("someValue")
		if err == nil {
			t.Error("We should have received an error")
			return
		}
		assert.Contains(t, err.Error(), "value dropped")
		sent <- true
	}()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-sent:
		t.Log("Call correctly dropped value")
	case <-timeout:
		t.Error("Call did not block")
	}
}

func TestNoBackpressureOnProducerWithEagerBroadcast(t *testing.T) {
	b := NewNonBlockingBroadcaster(0)
	var sent = make(chan bool, 1)
	go func() {
		b.SubmitBlocking("someValue")
		sent <- true
	}()
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		timeout <- true
	}()
	select {
	case <-sent:
		t.Log("Correct, no backpressure")
	case <-timeout:
		t.Error("Error, backpressure with eager broadcast")
	}
}
