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
func slowConsumer(channel chan *StateUpdate, wg *sync.WaitGroup) {
	go func() {
		for {
			<-channel
			wg.Done()
			time.Sleep(10 * time.Millisecond)
		}
	}()
}

func consumeState(channel <-chan *StateUpdate, wg *sync.WaitGroup) {
	go func(c <-chan *StateUpdate) {
		for {
			<-c
			wg.Done()
		}
	}(channel)
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

	b := NewNonBlockingStateBroadcaster(50, 0)

	var wg sync.WaitGroup
	wg.Add(2 * numberOfStateMessagesSent)
	blockingChan := make(chan *StateUpdate, 10)
	slowConsumer(blockingChan, &wg)

	nonBlockingChan := make(chan *StateUpdate, numberOfStateMessagesSent)
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

	b := NewNonBlockingStateBroadcaster(50, 0)

	chan1 := make(chan *StateUpdate, 20)
	chan2 := make(chan *StateUpdate, 20)

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
	assert.Contains(t, result, &StateUpdate{InitialState, "A3"})
	assert.Contains(t, result, &StateUpdate{InitialState, "B2"})
	assert.Contains(t, result, &StateUpdate{InitialState, "C1"})

	b.Submit("C", "C2")
	assert.Equal(t, &StateUpdate{Update, "C2"}, <-chan1)

	b.Register(chan2)

	result2 := consumeAvailableMessages(chan2)

	assert.Equal(t, 3, len(result2))
	assert.Contains(t, result2, &StateUpdate{InitialState, "A3"})
	assert.Contains(t, result2, &StateUpdate{InitialState, "B2"})
	assert.Contains(t, result2, &StateUpdate{InitialState, "C2"})

	b.Submit("B", "B3")
	assert.Equal(t, &StateUpdate{Update, "B3"}, <-chan1)
	assert.Equal(t, &StateUpdate{Update, "B3"}, <-chan2)

	b.Update("B", func(i interface{}) interface{} {
		assert.Equal(t, "B3", i.(string))
		return "B5"
	})
	assert.Equal(t, &StateUpdate{Update, "B5"}, <-chan1)
	assert.Equal(t, &StateUpdate{Update, "B5"}, <-chan2)

}

func consumeAvailableMessages(input chan *StateUpdate) []*StateUpdate {
	result := make([]*StateUpdate, 0, 10)
loop:
	for {
		select {
		case i := <-input:
			result = append(result, i)
		default:
			break loop
		}
	}
	return result
}

func TestTtl(t *testing.T) {
	b := NewNonBlockingStateBroadcaster(50, 1*time.Millisecond)

	chan1 := make(chan *StateUpdate, 20)

	b.Register(chan1)

	b.Submit("A", "A1")
	b.Submit("A", "A2")
	b.Submit("B", "B1")

	time.Sleep(2 * time.Millisecond)

	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 3, len(result))
	assert.Contains(t, result, &StateUpdate{Update, "A1"})
	assert.Contains(t, result, &StateUpdate{Update, "A2"})
	assert.Contains(t, result, &StateUpdate{Update, "B1"})

	result2 := b.GetCurrentState()

	assert.Equal(t, 0, len(result2)) // state has expired

}

func TestDelete(t *testing.T) {

	b := NewNonBlockingStateBroadcaster(50, 0)

	chan1 := make(chan *StateUpdate, 20)
	chan2 := make(chan *StateUpdate, 20)

	b.Submit("A", "A1")
	b.Submit("B", "B1")
	time.Sleep(50 * time.Millisecond)

	b.Register(chan1)
	result := consumeAvailableMessages(chan1)

	assert.Equal(t, 2, len(result))
	assert.Contains(t, result, &StateUpdate{InitialState, "A1"})
	assert.Contains(t, result, &StateUpdate{InitialState, "B1"})

	b.Delete("A")
	time.Sleep(50 * time.Millisecond)
	result = consumeAvailableMessages(chan1)
	assert.Equal(t, 1, len(result))
	assert.Contains(t, result, &StateUpdate{Delete, "A"})
	b.Register(chan2)
	result2 := b.GetCurrentState()

	assert.Equal(t, 1, len(result2))
	assert.Equal(t, result2["B"], "B1")

}

func TestStateCleared(t *testing.T) {

	b := NewNonBlockingStateBroadcaster(50, 0)

	b.Submit("A", "A1")
	time.Sleep(50 * time.Millisecond)

	result := b.GetCurrentState()

	assert.Equal(t, 1, len(result))
	assert.Equal(t, result["A"], "A1")

	b.ClearState()
	result2 := b.GetCurrentState()

	assert.Equal(t, 0, len(result2))

}
