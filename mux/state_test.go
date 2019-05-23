package mux

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

const blockingConsumerName = "blocking"

// this function will consume the given amount of messages and block
func slowConsumer(channel <-chan interface{}, wg *sync.WaitGroup) {
	go func() {
		for {
			<-channel
			wg.Done()
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func consumeState(channel <-chan interface{}, wg *sync.WaitGroup) {
	go func(c <-chan interface{}) {
		for {
			<-c
			wg.Done()
		}
	}(channel)
}

var keyExtractor = func(f interface{}) interface{} {
	return f.(string)[0]
}

func countDownOnBackpressure(consumerName string, consumer chan string, wg *sync.WaitGroup) func(config *ConsumerConfig) error {
	return func(config *ConsumerConfig) error {
		config.OnBackpressure(func(value interface{}) {
			fmt.Println("on back pressure " + consumerName)
			consumer <- consumerName
			wg.Done()
		})
		return nil
	}
}

func TestBackpressureOnStateBroadcaster(t *testing.T) {
	const numberOfStateMessagesSent = 20
	var blockingClientChan = make(chan string, numberOfStateMessagesSent)
	var nonBlockingClientChan = make(chan string, numberOfStateMessagesSent)

	b, err := NewNonBlockingStateBroadcaster(50, 0)

	if err != nil {
		t.Fail()
	}

	var wg sync.WaitGroup
	wg.Add(2 * numberOfStateMessagesSent)
	blockingChan := make(chan interface{}, 10)
	slowConsumer(blockingChan, &wg)

	nonBlockingChan := make(chan interface{}, numberOfStateMessagesSent)
	consumeState(nonBlockingChan, &wg)

	b.Register(blockingChan, countDownOnBackpressure(blockingConsumerName, blockingClientChan, &wg))
	b.Register(nonBlockingChan, countDownOnBackpressure("non-blocking", nonBlockingClientChan, &wg))

	for i := 0; i < numberOfStateMessagesSent; i++ {
		b.Submit("key", fmt.Sprintf("value %d", i))
	}

	fmt.Println("Waiting for all messages to be consumed")
	wg.Wait()
	fmt.Println("Waiting for all messages to be consumed --> DONE")
	close(blockingClientChan)

	fmt.Println("counting the number of times backpressure was invoked ")

	backpressureCount := 0
	for bc := range blockingClientChan {
		assert.Equal(t, blockingConsumerName, bc)
		backpressureCount++
	}
	assert.True(t, backpressureCount >= 1) // since it has a small buffer, the blocking consumer might be blocking even before it starts to sleep
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

func TestDelete(t *testing.T) {

	b, _ := NewNonBlockingStateBroadcaster(50, 0)

	chan1 := make(chan interface{}, 20)
	chan2 := make(chan interface{}, 20)

	b.Submit("A", "A1")
	b.Submit("B", "B1")
	time.Sleep(50 * time.Millisecond)

	b.Register(chan1)
	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, "A1")
	assert.Contains(t, result, "B1")

	b.Delete("A")
	b.Register(chan2)
	result2 := consumeAvailableMessages(chan2)

	assert.Equal(t, 1, len(result2))
	assert.Contains(t, result, "B1")

}

func TestStateCleared(t *testing.T) {

	b, _ := NewNonBlockingStateBroadcaster(50, 0)

	chan1 := make(chan interface{}, 20)
	chan2 := make(chan interface{}, 20)

	b.Submit("A", "A1")
	time.Sleep(50 * time.Millisecond)

	b.Register(chan1)
	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 1, len(result))
	assert.Contains(t, result, "A1")

	b.ClearState()
	b.Register(chan2)
	result2 := consumeAvailableMessages(chan2)

	assert.Equal(t, 0, len(result2))

}
