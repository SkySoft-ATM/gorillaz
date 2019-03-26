package mux

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const numberOfMessagesSent = 20

var onBackPressure = func(consumerName string, value interface{}) {
	fmt.Println("on back pressure " + consumerName)
	blockingClientChan <- consumerName
}

var blockingClientChan = make(chan string, numberOfMessagesSent+1)
var finished = make(chan bool, 1)

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

func consume(channel <-chan interface{}, numberOfMessages int) {
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

func TestBackpressureOnBroadcaster(t *testing.T) {

	b := NewNonBlockingBroadcaster(50, onBackPressure)

	blockingChan := make(chan interface{}, 10)
	consumeAndBlock(5, blockingChan)

	normalChan := make(chan interface{}, numberOfMessagesSent+1)
	consume(normalChan, numberOfMessagesSent)

	b.Register(blockingConsumerName, blockingChan)
	b.Register("non-blocking", normalChan)
	fmt.Println("submitting messages")
	for i := 0; i < numberOfMessagesSent; i++ {
		b.Submit(fmt.Sprintf("value %d", i))
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
