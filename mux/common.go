package mux

import "time"

type registration struct {
	consumer consumer
	done     chan<- bool
}

type unregistration struct {
	channel chan<- interface{}
	done    chan<- bool
}

type consumer struct {
	config  ConsumerConfig
	channel chan<- interface{}
}

type ttlValue struct {
	expiresAt time.Time
	value     interface{}
}

type BroadcasterConfig struct {
	postBroadcast  func(interface{})
	eagerBroadcast bool
}

type ConsumerConfig struct {
	onBackpressure func(value interface{})
}

type BroadcasterOptionFunc func(*BroadcasterConfig) error

type ConsumerOptionFunc func(*ConsumerConfig) error

func (s *ConsumerConfig) OnBackpressure(onBackpressure func(value interface{})) {
	s.onBackpressure = onBackpressure
}

// Defines an action that will be done once the value has been broadcasted.
func (b *BroadcasterConfig) PostBroadcast(postBroadcast func(interface{})) {
	b.postBroadcast = postBroadcast
}

// If true, the broadcaster will start broadcasting eagerly, otherwise the first consumer will trigger the broadcast.
// A lazy broadcast can be used to apply backpressure on the producer if no consumer is present.
func (b *BroadcasterConfig) EagerBroadcast(eager bool) {
	b.eagerBroadcast = eager
}

var LazyBroadcast BroadcasterOptionFunc = func(bc *BroadcasterConfig) error {
	bc.eagerBroadcast = false
	return nil
}
