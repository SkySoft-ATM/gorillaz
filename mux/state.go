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

type StateUpdateChan chan<- *StateUpdate

type clearAll string

const clearAllValues clearAll = "ALL"

type StateBroadcaster struct {
	input   chan keyValue
	delete  chan interface{}
	reg     chan stateRegistration
	unreg   chan stateUnregistration
	outputs map[StateUpdateChan]ConsumerConfig
	state   map[interface{}]ttlValue
	*BroadcasterConfig
}

type UpdateType int

const (
	Update UpdateType = iota
	InitialState
	Delete
)

type StateUpdate struct {
	UpdateType UpdateType
	Value      interface{}
}

func (su *StateUpdate) IsDelete() bool {
	return su.UpdateType == Delete
}

// Register a new channel to receive broadcasts
func (b *StateBroadcaster) Register(newch StateUpdateChan, options ...ConsumerOptionFunc) error {
	done := make(chan bool)
	config := &ConsumerConfig{}
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	b.reg <- stateRegistration{stateConsumer{*config, newch}, done}
	<-done
	return nil
}

type stateRegistration struct {
	consumer stateConsumer
	done     chan<- bool
}

type stateUnregistration struct {
	channel StateUpdateChan
	done    chan<- bool
}

type stateConsumer struct {
	config  ConsumerConfig
	channel StateUpdateChan
}

// Unregister a channel so that it no longer receives broadcasts.
func (b *StateBroadcaster) Unregister(newch StateUpdateChan) {
	done := make(chan bool)
	b.unreg <- stateUnregistration{newch, done}
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

// Deletes all entries from the state
func (b *StateBroadcaster) ClearState() {
	if b != nil {
		b.delete <- clearAllValues
	}
}

func (b *StateBroadcaster) broadcast(m *StateUpdate) {
	for ch := range b.outputs {
		select {
		case ch <- m:
			//message sent
		default:
			//consumer is not ready to receive a message, drop it and execute provided action on backpressure
			config := b.outputs[ch]
			if config.onBackpressure != nil {
				config.onBackpressure(m)
			}
		}
	}
}

func (b *StateBroadcaster) run(ttl time.Duration) {
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
			if _, isClearAll := k.(clearAll); isClearAll {
				for k := range b.state {
					b.broadcast(&StateUpdate{Delete, k})
				}
				b.state = make(map[interface{}]ttlValue)
			} else {
				delete(b.state, k)
				b.broadcast(&StateUpdate{Delete, k})
			}
		case r, ok := <-b.reg:
			if ok {
				b.outputs[r.consumer.channel] = r.consumer.config
				for _, v := range b.state {
					select {
					case r.consumer.channel <- &StateUpdate{InitialState, v.value}:
					//sent
					default:
						if r.consumer.config.onBackpressure != nil {
							r.consumer.config.onBackpressure(v.value)
						}
					}

				}
				r.done <- true
			} else {
				// close all registered output channel to notify them that the StateBroadcaster is closed
				for output := range b.outputs {
					close(output)
				}
				ticker.Stop() //to avoid goroutine leak
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
			b.broadcast(&StateUpdate{Update, m.value})
		}
	}
}

// NewBroadcaster creates a new StateBroadcaster with the given input channel buffer length.
// ttl defines a time to live for values sent to the state broadcaster, 0 means no expiry
func NewNonBlockingStateBroadcaster(bufLen int, ttl time.Duration, options ...BroadcasterOptionFunc) (*StateBroadcaster, error) {
	b := &StateBroadcaster{
		input:             make(chan keyValue, bufLen),
		reg:               make(chan stateRegistration),
		delete:            make(chan interface{}),
		unreg:             make(chan stateUnregistration),
		outputs:           make(map[StateUpdateChan]ConsumerConfig),
		state:             make(map[interface{}]ttlValue),
		BroadcasterConfig: &BroadcasterConfig{},
	}
	for _, option := range options {
		if err := option(b.BroadcasterConfig); err != nil {
			return nil, err
		}
	}

	go b.run(ttl)
	return b, nil
}
