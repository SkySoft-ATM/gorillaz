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
	"sync/atomic"
)

type Broadcaster struct {
	closeReq chan chan struct{}
	input    chan interface{}
	reg      chan registration
	unreg    chan unregistration
	outputs  map[chan<- interface{}]ConsumerConfig
	*BroadcasterConfig
	closed uint32
}

// Register a new channel to receive broadcasts
func (b *Broadcaster) Register(newch chan<- interface{}, options ...ConsumerOptionFunc) {
	done := make(chan struct{})
	config := &ConsumerConfig{}
	for _, option := range options {
		if err := option(config); err != nil {
			panic("failed to register to broadcaster, option returned an error, " + err.Error())
		}
	}
	b.reg <- registration{consumer{*config, newch}, done}
	<-done
}

// Unregister a channel so that it no longer receives broadcasts.
func (b *Broadcaster) Unregister(newch chan<- interface{}) {
	done := make(chan struct{})
	b.unreg <- unregistration{newch, done}
	<-done
}

// Shut this StateBroadcaster down.
func (b *Broadcaster) Close() {
	atomic.StoreUint32(&b.closed, 1)
	closed := make(chan struct{})
	b.closeReq <- closed
	<-closed
}

func (b *Broadcaster) Closed() bool {
	return atomic.LoadUint32(&b.closed) > 0
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
			if subConfig.disconnectOnBackpressure {
				b.unregister(ch)
			}
		}
	}
	if b.postBroadcast != nil {
		b.postBroadcast(m)
	}
}

// onBackPressureState can be nil
func (b *Broadcaster) run() {
	for {
		// if lazy, if there is no more subscriber, do not consume any value until there is at least 1 subscriber
		if !b.eagerBroadcast && len(b.outputs) == 0 {
			select {
			case u := <-b.unreg:
				// there is currently no registration
				u.done <- struct{}{}
			case r := <-b.reg:
				b.addSubscriber(r)
			case closed := <-b.closeReq:
				closed <- struct{}{}
				return
			}
		} else {
			select {
			case closed := <-b.closeReq:
				close(b.input)
				if len(b.outputs) == 0 {
					closed <- struct{}{}
					return
				}
				// if there are still messages to broadcast, do it
				for m := range b.input {
					b.broadcast(m)
				}
				// close all subscribers
				for sub := range b.outputs {
					close(sub)
				}
				// cleanup b.outputs
				for sub := range b.outputs {
					delete(b.outputs, sub)
				}
				closed <- struct{}{}
				return
			case r := <-b.reg:
				b.addSubscriber(r)
			case u := <-b.unreg:
				b.unregister(u.channel)
				u.done <- struct{}{}
			case m := <-b.input:
				b.broadcast(m)
			}
		}
	}
}

func (b *Broadcaster) unregister(ch chan<- interface{}) {
	// check if the channel was not already unregistered
	if _, ok := b.outputs[ch]; ok {
		delete(b.outputs, ch)
		close(ch)
	}
}

func (b *Broadcaster) addSubscriber(r registration) {
	b.outputs[r.consumer.channel] = r.consumer.config
	r.done <- struct{}{}
}

// NewBroadcaster creates a new Broadcaster with the given input channel buffer length.
// onBackPressureState is an action to execute when messages are dropped on back pressure (typically logging), it can be nil
func NewNonBlockingBroadcaster(bufLen int, options ...BroadcasterOptionFunc) *Broadcaster {
	b := &Broadcaster{
		closeReq:          make(chan chan struct{}),
		input:             make(chan interface{}, bufLen),
		reg:               make(chan registration),
		unreg:             make(chan unregistration),
		outputs:           make(map[chan<- interface{}]ConsumerConfig),
		BroadcasterConfig: &BroadcasterConfig{eagerBroadcast: true},
	}

	for _, option := range options {
		option(b.BroadcasterConfig)
	}

	go b.run()
	return b
}
