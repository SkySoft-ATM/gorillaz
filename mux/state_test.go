package mux

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const blockingConsumerName = "blocking"

// this function will consume the given amount of messages and block
func consumeAndBlockState(amountToConsume int, channel <-chan interface{}) {
	go func(c <-chan interface{}) {
		for i := 0; i < amountToConsume; i++ {
			<-c
		}
		fmt.Println("consumeAndBlock is now blocking")
		time.Sleep(30000 * time.Second)
	}(channel)
}

func consumeState(channel <-chan interface{}, numberOfMessages int, finished chan<- bool) {
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

var keyExtractor = func(f interface{}) interface{} {
	return f.(string)[0]
}

func TestBackpressureOnStateBroadcaster(t *testing.T) {
	const numberOfStateMessagesSent = 20
	var blockingClientChan = make(chan string, numberOfStateMessagesSent+1)
	var nonBlockingClientChan = make(chan string, numberOfStateMessagesSent+1)
	var finished = make(chan bool, 1)

	b, err := NewNonBlockingStateBroadcaster(50, 0)

	if err != nil {
		t.Fail()
	}

	blockingChan := make(chan interface{}, 10)
	consumeAndBlockState(5, blockingChan)

	nonBlockingChan := make(chan interface{}, numberOfStateMessagesSent+1)
	consumeState(nonBlockingChan, numberOfStateMessagesSent, finished)

	b.Register(blockingChan, backpressureOptionForConsumer(blockingConsumerName, blockingClientChan))
	b.Register(nonBlockingChan, backpressureOptionForConsumer("non-blocking", nonBlockingClientChan))
	fmt.Println("submitting messages")
	for i := 0; i < numberOfStateMessagesSent; i++ {
		b.Submit("key", fmt.Sprintf("value %d", i))
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

func TestFullStateSentToSubscriber(t *testing.T) {

	b, _ := NewNonBlockingStateBroadcaster(50, 0)

	chan1 := make(chan interface{}, 20)
	chan2 := make(chan interface{}, 20)

	b.Submit("A", "A1")
	b.Submit("A", "A2")
	b.Submit("B", "B1")
	b.Submit("A", "A3")
	b.Submit("C", "C1")
	b.Submit("B", "B2")
	time.Sleep(500 * time.Millisecond)

	b.Register(chan1)

	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 3, len(result))
	assert.Contains(t, result, "A3")
	assert.Contains(t, result, "B2")
	assert.Contains(t, result, "C1")

	b.Submit("C", "C2")
	assert.Equal(t, "C2", <-chan1)

	b.Register(chan2)

	result2 := consumeAvailableMessages(chan2)

	assert.Equal(t, 3, len(result2))
	assert.Contains(t, result2, "A3")
	assert.Contains(t, result2, "B2")
	assert.Contains(t, result2, "C2")

	b.Submit("B", "B3")
	assert.Equal(t, "B3", <-chan1)
	assert.Equal(t, "B3", <-chan2)

}

func consumeAvailableMessages(input chan interface{}) []string {
	result := make([]string, 0, 10)
loop:
	for {
		select {
		case i := <-input:
			result = append(result, i.(string))
		default:
			break loop
		}
	}
	return result
}

func TestTtl(t *testing.T) {
	b, _ := NewNonBlockingStateBroadcaster(50, 1*time.Millisecond)

	chan1 := make(chan interface{}, 20)
	chan2 := make(chan interface{}, 20)

	b.Register(chan1)

	b.Submit("A", "A1")
	b.Submit("A", "A2")
	b.Submit("B", "B1")

	time.Sleep(2 * time.Millisecond)

	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 3, len(result))
	assert.Contains(t, result, "A1")
	assert.Contains(t, result, "A2")
	assert.Contains(t, result, "B1")

	b.Register(chan2)

	result2 := consumeAvailableMessages(chan2)

	assert.Equal(t, 0, len(result2)) // state has expired

}
