package mux

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestDisconnectOnBackPressure(t *testing.T) {
	b := NewNonBlockingBroadcaster(0)
	ch1 := make(chan interface{}, 1)
	ch2 := make(chan interface{}, 1)

	b.Register(ch1, DisconnectOnBackPressure())
	b.Register(ch2)

	b.SubmitBlocking(1)

	// ch1  and ch2 should be open with 1 element
	found, i, open := consume(ch1)
	assert.Equal(t, found, true)
	assert.Equal(t, i, 1)
	assert.Equal(t, open, true)

	found, i, open = consume(ch2)
	assert.Equal(t, found, true)
	assert.Equal(t, i, 1)
	assert.Equal(t, open, true)

	// now ch1 and ch2 should be empty
	found, _, open = consume(ch1)
	assert.Equal(t, found, false)
	assert.Equal(t, open, true)

	found, _, open = consume(ch2)
	assert.Equal(t, found, false)
	assert.Equal(t, open, true)

	// put back 2 elements inside
	b.SubmitBlocking(3)
	b.SubmitBlocking(4)

	// there is place for only 1 element in ch1 and ch2
	// ch1 has DisconnectOnBackpressure, so the channel should be closed
	found, i, open = consume(ch1)
	assert.Equal(t, true, found)
	assert.Equal(t, 3, i)
	assert.Equal(t, true, open)

	found, i, open = consume(ch1)
	assert.Equal(t, true, found)
	assert.Equal(t, nil, i)
	assert.Equal(t, false, open)

	// ch2 does not have DisconnectOnBackpressure, so it should still be open, with 1 element inside
	found, i, open = consume(ch2)
	assert.Equal(t, true, found)
	assert.Equal(t, 3, i)
	assert.Equal(t, true, open)

	found, i, open = consume(ch2)
	assert.Equal(t, false, found)
	assert.Equal(t, nil, i)
	assert.Equal(t, true, open)
}

func consume(c chan interface{}) (found bool, value interface{}, open bool) {
	select {
	case v, ok := <-c:
		return true, v, ok
	case <-time.After(500 * time.Millisecond):
		return false, nil, true
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

func TestUnsubscribeAfterClose(t *testing.T) {
	b := NewNonBlockingBroadcaster(0)
	receiver := make(chan interface{})
	b.Register(receiver)

	b.Close()

	timer := time.NewTimer(1 * time.Second)

	done := make(chan interface{})

	go func() {
		b.Unregister(receiver)
		done <- struct{}{}
	}()

	select {
	case <-timer.C:
		t.Fatalf("unable to unregister on time")
	case <-done:
		t.Log("Unregistered successfully")
	}
}
