/*
Provides pubsub of messages over channels.
A provider has a broadcaster into which it Submits messages and into
which subscribers Register to pick up those messages.

If one of the subscribers is not able to consume the message, then messages will be dropped for this consumer.
It is possible to pass a function to handle dropped messages.

Once a broadcaster is closed, SubmitBlocking will panic and SubmitNonBlocking returns an error

*/
package mux

import (
	"fmt"
	"sync/atomic"
)

type Broadcaster struct {
	closing  uint32
	closeReq chan struct{}
	input    chan interface{}
	reg      chan registration
	unreg    chan unregistration
	outputs  map[chan<- interface{}]ConsumerConfig
	*BroadcasterConfig
	closed chan interface{}
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
	select {
	case b.unreg <- unregistration{newch, done}:
		<-done
	case <-b.closed:
		return
	}
}

// Shut this StateBroadcaster down.
func (b *Broadcaster) Close() {
	// mark the broadcaster as closing, so we don't attempt to close it twice
	alreadyClosing := !atomic.CompareAndSwapUint32(&b.closing, 0, 1)
	if alreadyClosing {
		// already closing
		<-b.closed
		return
	}
	b.closeReq <- struct{}{}
	<-b.closed
}

func (b *Broadcaster) Closed() bool {
	select {
	case <-b.closed:
		return true
	default:
		return false
	}
}

// Submit a new object to all subscribers, this call can block if the input channel is full
func (b *Broadcaster) SubmitBlocking(i interface{}) {
	if closing := atomic.LoadUint32(&b.closing); closing > 0 {
		panic("writing to a closing broadcaster")
	}
	b.input <- i
}

// Submit a new object to all subscribers, this call will drop the message if the input channel is full
func (b *Broadcaster) SubmitNonBlocking(i interface{}) error {
	if closing := atomic.LoadUint32(&b.closing); closing > 0 {
		return fmt.Errorf("writing to a closing broadcaster")
	}
	select {
	// try to insert the message into the broadcaster.
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
			case <-b.closeReq:
				// notify all listeners that the broadcaster is now closed
				close(b.closed)
				return
			}
		} else {
			select {
			case <-b.closeReq:
				// notify all listeners that the broadcaster is now closed
				close(b.closed)

				// finish delivering messages to subscribers before terminating
				if len(b.outputs) > 0 {
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
				}
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
		closing:           0,
		closeReq:          make(chan struct{}),
		input:             make(chan interface{}, bufLen),
		reg:               make(chan registration),
		unreg:             make(chan unregistration),
		outputs:           make(map[chan<- interface{}]ConsumerConfig),
		BroadcasterConfig: &BroadcasterConfig{eagerBroadcast: true},
		closed:            make(chan interface{}),
	}

	for _, option := range options {
		option(b.BroadcasterConfig)
	}

	go b.run()
	return b
}
