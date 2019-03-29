package mux

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// this function will consume the given amount of messages and block
func consumeAndBlock(amountToConsume int, channel <-chan interface{}) {
	go func(c <-chan interface{}) {
		for i := 0; i < amountToConsume; i++ {
			<-c
		}
		fmt.Println("consumeAndBlock is now blocking")
		time.Sleep(30000 * time.Second)
	}(channel)
}

func consume(channel <-chan interface{}, numberOfMessages int, finished chan<- bool) {
	go func(c <-chan interface{}) {
		i := 0
		for {
			<-c
			i++
			if i == numberOfMessages {
				finished <- true
			}
		}
	}(channel)
}

func backpressureForConsumer(consumerName string, backpressureConsumer chan string) func(value interface{}) {
	return func(value interface{}) {
		fmt.Println("on back pressure " + consumerName)
		backpressureConsumer <- consumerName
	}
}

func backpressureOptionForConsumer(consumerName string, backpressureConsumer chan string) func(config *ConsumerConfig) error {
	return func(config *ConsumerConfig) error {
		config.OnBackpressure(backpressureForConsumer(consumerName, backpressureConsumer))
		return nil
	}
}

func TestBackpressureOnConsumer(t *testing.T) {

	const numberOfMessagesSent = 20
	var blockingClientChan = make(chan string, numberOfMessagesSent+1)
	var nonBlockingClientChan = make(chan string, numberOfMessagesSent+1)
	var finished = make(chan bool, 1)

	b, err := NewNonBlockingBroadcaster(50)

	failIfError(err, t)

	blockingChan := make(chan interface{}, 10)
	consumeAndBlock(5, blockingChan)

	nonBlockingChan := make(chan interface{}, numberOfMessagesSent+1)
	consume(nonBlockingChan, numberOfMessagesSent, finished)

	err = b.Register(blockingChan, backpressureOptionForConsumer(blockingConsumerName, blockingClientChan))
	failIfError(err, t)
	b.Register(nonBlockingChan, backpressureOptionForConsumer("non-blocking", nonBlockingClientChan))
	failIfError(err, t)
	fmt.Println("submitting messages")
	for i := 0; i < numberOfMessagesSent; i++ {
		b.SubmitBlocking(fmt.Sprintf("value %d", i))
	}
	fmt.Println("wait until the non blocking consumer consumes everything ")
	<-finished
	close(blockingClientChan)

	fmt.Println("counting the number of times backpressure was invoked ")

	backpressureCount := 0
	for bc := range blockingClientChan {
		assert.Equal(t, blockingConsumerName, bc)
		backpressureCount++
	}
	assert.True(t, backpressureCount >= 5) // since it has a small buffer, the blocking consumer might be blocking even before it starts to sleep
	t.Log(fmt.Sprintf("backpressure count = %d", backpressureCount))
}

func failIfError(err error, t *testing.T) {
	if err != nil {
		t.Fail()
	}
}

func TestBackpressureOnProducer(t *testing.T) {
	b, err := NewNonBlockingBroadcaster(0, LazyBroadcast)
	failIfError(err, t)
	var sent = make(chan bool, 1)
	go func() {
		_ = b.SubmitBlocking("someValue")
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
	b, err := NewNonBlockingBroadcaster(0, LazyBroadcast)
	failIfError(err, t)
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
	b, err := NewNonBlockingBroadcaster(0)
	failIfError(err, t)
	var sent = make(chan bool, 1)
	go func() {
		err = b.SubmitBlocking("someValue")
		failIfError(err, t)
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
