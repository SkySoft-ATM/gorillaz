/*
Provides pubsub of messages over channels.
A provider has a broadcaster into which it Submits messages and into
which subscribers Register to pick up those messages.

If one of the subscribers is not able to consume the message, then messages will be dropped for this consumer.
It is possible to pass a function to handle dropped messages.

*/
package mux

import (
	"fmt"
)

type Broadcaster struct {
	input   chan interface{}
	reg     chan registration
	unreg   chan unregistration
	outputs map[chan<- interface{}]string
}

// Register a new channel to receive broadcasts
func (b *Broadcaster) Register(consumerName string, newch chan<- interface{}) {
	done := make(chan bool)
	b.reg <- registration{consumer{consumerName, newch}, done}
	<-done
}

// Unregister a channel so that it no longer receives broadcasts.
func (b *Broadcaster) Unregister(newch chan<- interface{}) {
	done := make(chan bool)
	b.unreg <- unregistration{newch, done}
	<-done
}

// Shut this StateBroadcaster down.
func (b *Broadcaster) Close() error {
	close(b.reg)
	return nil
}

// Submit a new object to all subscribers
func (b *Broadcaster) Submit(i interface{}) error {
	if b != nil && i != nil {
		b.input <- i
		return nil
	}
	return fmt.Errorf("nil value")
}

func (b *Broadcaster) broadcast(m interface{}, onBackPressure func(consumerName string, value interface{})) {
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
func (b *Broadcaster) run(onBackPressure func(consumerName string, value interface{})) {
	for {
		select {
		case r, ok := <-b.reg:
			if ok {
				b.outputs[r.consumer.channel] = r.consumer.name
				r.done <- true
			} else {
				return
			}
		case u := <-b.unreg:
			delete(b.outputs, u.channel)
			u.done <- true
		case m := <-b.input:
			b.broadcast(m, onBackPressure)

		}
	}
}

// NewBroadcaster creates a new Broadcaster with the given input channel buffer length.
// onBackPressureState is an action to execute when messages are dropped on back pressure (typically logging), it can be nil
func NewNonBlockingBroadcaster(buflen int, onBackPressure func(consumerName string, value interface{})) *Broadcaster {
	b := &Broadcaster{
		input:   make(chan interface{}, buflen),
		reg:     make(chan registration),
		unreg:   make(chan unregistration),
		outputs: make(map[chan<- interface{}]string),
	}
	go b.run(onBackPressure)
	return b
}
