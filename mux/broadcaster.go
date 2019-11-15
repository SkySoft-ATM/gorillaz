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
	closeReq chan chan bool
	input    chan interface{}
	reg      chan registration
	unreg    chan unregistration
	outputs  map[chan<- interface{}]ConsumerConfig
	*BroadcasterConfig
}

// Register a new channel to receive broadcasts
func (b *Broadcaster) Register(newch chan<- interface{}, options ...ConsumerOptionFunc) error {
	done := make(chan bool)
	config := &ConsumerConfig{}
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	b.reg <- registration{consumer{*config, newch}, done}
	<-done
	return nil
}

// Unregister a channel so that it no longer receives broadcasts.
func (b *Broadcaster) Unregister(newch chan<- interface{}) {
	done := make(chan bool)
	b.unreg <- unregistration{newch, done}
	<-done
}

// Shut this StateBroadcaster down.
func (b *Broadcaster) Close() {
	closed := make(chan bool)
	b.closeReq <- closed
	<-closed
}

// Submit a new object to all subscribers, this call can block if the input channel is full
func (b *Broadcaster) SubmitBlocking(i interface{}) {
	b.input <- i
}

// Submit a new object to all subscribers, this call will drop the message if the input channel is full
func (b *Broadcaster) SubmitNonBlocking(i interface{}) error {
	select {
	case b.input <- i:
		return nil
	default:
		return fmt.Errorf("value dropped")
	}
}

func (b *Broadcaster) broadcast(m interface{}) {
	for ch := range b.outputs {
		select {
		case ch <- m:
			//message sent
		default:
			//consumer is not ready to receive a message, drop it and execute provided action on backpressure
			subConfig := b.outputs[ch]
			if subConfig.onBackpressure != nil {
				subConfig.onBackpressure(m)
			}
		}
	}
	if b.postBroadcast != nil {
		b.postBroadcast(m)
	}
}

// onBackPressureState can be nil
func (b *Broadcaster) run() {
	subscriberCount := 0

waitForSub:
	// if lazy, wait for the first registration before doing anything
	if !b.eagerBroadcast {
		select {
		case r := <-b.reg:
			subscriberCount = b.addSubscriber(r, subscriberCount)
		case closed := <-b.closeReq:
			closed <- true
			return
		}
	}

	for {
		select {
		case closed := <-b.closeReq:
			close(b.input)
			// if there are still messages to broadcast, do it
			for m := range b.input {
				b.broadcast(m)
			}
			// close all subscribers
			for sub := range b.outputs {
				close(sub)
			}
			closed <- true
			return
		case r := <-b.reg:
			subscriberCount = b.addSubscriber(r, subscriberCount)
		case u := <-b.unreg:
			delete(b.outputs, u.channel)
			subscriberCount--
			u.done <- true

			// if lazy, if there is no more subscriber, block until there is at least 1 subscriber
			if !b.eagerBroadcast && subscriberCount == 0 {
				goto waitForSub
			}
		case m := <-b.input:
			b.broadcast(m)
		}
	}
}

func (b *Broadcaster) addSubscriber(r registration, subscriberCount int) int {
	b.outputs[r.consumer.channel] = r.consumer.config
	r.done <- true
	return subscriberCount + 1
}

// NewBroadcaster creates a new Broadcaster with the given input channel buffer length.
// onBackPressureState is an action to execute when messages are dropped on back pressure (typically logging), it can be nil
func NewNonBlockingBroadcaster(bufLen int, options ...BroadcasterOptionFunc) (*Broadcaster, error) {
	b := &Broadcaster{
		closeReq:          make(chan chan bool),
		input:             make(chan interface{}, bufLen),
		reg:               make(chan registration),
		unreg:             make(chan unregistration),
		outputs:           make(map[chan<- interface{}]ConsumerConfig),
		BroadcasterConfig: &BroadcasterConfig{eagerBroadcast: true},
	}

	for _, option := range options {
		if err := option(b.BroadcasterConfig); err != nil {
			return nil, err
		}
	}

	go b.run()
	return b, nil
}
