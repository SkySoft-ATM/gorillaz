/*
Provides pubsub of messages over channels.
A provider has a broadcaster into which it Submits messages and into
which subscribers Register to pick up those messages.

If one of the subscribers is not able to consume the message, then messages will be dropped for this consumer.
It is possible to pass a function to handle dropped messages.

The broadcaster is stateful: it keeps the last submitted value per key.
When a new subscriber registers, it will receive the full state before receiving live updates.
For this reason it is important that subscribers provide a channel with a buffer size that is big enough to receive
the full state immediately, otherwise values will be dropped on backpressure.

*/
package mux

import (
	"fmt"
	"time"
)

type keyValue struct {
	key   interface{}
	value interface{}
}

type StateBroadcaster struct {
	input   chan keyValue
	delete  chan interface{}
	reg     chan registration
	unreg   chan unregistration
	outputs map[chan<- interface{}]string
	state   map[interface{}]ttlValue
}

// Register a new channel to receive broadcasts
func (b *StateBroadcaster) Register(consumerName string, newch chan<- interface{}) {
	done := make(chan bool)
	b.reg <- registration{consumer{consumerName, newch}, done}
	<-done
}

// Unregister a channel so that it no longer receives broadcasts.
func (b *StateBroadcaster) Unregister(newch chan<- interface{}) {
	done := make(chan bool)
	b.unreg <- unregistration{newch, done}
	<-done
}

// Shut this StateBroadcaster down.
func (b *StateBroadcaster) Close() error {
	close(b.reg)
	return nil
}

// Submit a new object to all subscribers
func (b *StateBroadcaster) Submit(k interface{}, v interface{}) error {
	if b != nil && k != nil {
		b.input <- keyValue{k, v}
		return nil
	}
	return fmt.Errorf("nil key")
}

// Deletes the object associated with the given key from the state
func (b *StateBroadcaster) Delete(k interface{}) {
	if b != nil && k != nil {
		b.delete <- k
	}
}

func (b *StateBroadcaster) broadcast(m interface{}, onBackPressure func(consumerName string, value interface{})) {
	for ch := range b.outputs {
		select {
		case ch <- m:
			//message sent
		default:
			//consumer is not ready to receive a message, drop it and execute provided action on backpressure
			if onBackPressure != nil {
				onBackPressure(b.outputs[ch], m)
			}
		}
	}
}

// onBackPressureState can be nil
func (b *StateBroadcaster) run(ttl time.Duration, onBackPressure func(consumerName string, value interface{})) {
	var ticker time.Ticker
	if ttl > 0 {
		ticker = *time.NewTicker(ttl / 2)
	}
	for {
		select {
		case t := <-ticker.C:
			for k, v := range b.state {
				if !v.expiresAt.IsZero() && v.expiresAt.After(t) {
					delete(b.state, k)
				}
			}
		case k := <-b.delete:
			delete(b.state, k)
		case r, ok := <-b.reg:
			if ok {
				b.outputs[r.consumer.channel] = r.consumer.name
				for _, v := range b.state {
					select {
					case r.consumer.channel <- v.value:
					//sent
					default:
						onBackPressure(r.consumer.name, v.value)
					}

				}
				r.done <- true
			} else {
				return
			}
		case u := <-b.unreg:
			delete(b.outputs, u.channel)
			u.done <- true
		case m := <-b.input:
			key := m.key
			var expiresAt time.Time
			if ttl != 0 {
				expiresAt = time.Now().Add(ttl)
			}
			b.state[key] = ttlValue{expiresAt: expiresAt, value: m.value}
			b.broadcast(m.value, onBackPressure)

		}
	}
}

// NewBroadcaster creates a new StateBroadcaster with the given input channel buffer length.
// onBackPressureState is an action to execute when messages are dropped on back pressure (typically logging), it can be nil
func NewNonBlockingStateBroadcaster(bufLen int, ttl time.Duration, onBackPressure func(consumerName string, value interface{})) *StateBroadcaster {
	b := &StateBroadcaster{
		input:   make(chan keyValue, bufLen),
		reg:     make(chan registration),
		unreg:   make(chan unregistration),
		outputs: make(map[chan<- interface{}]string),
		state:   make(map[interface{}]ttlValue),
	}
	go b.run(ttl, onBackPressure)
	return b
}
