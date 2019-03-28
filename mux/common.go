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
	postBroadcast func(interface{})
}

type ConsumerConfig struct {
	onBackpressure func(value interface{})
}

type BroadcasterOptionFunc func(*BroadcasterConfig) error

type ConsumerOptionFunc func(*ConsumerConfig) error

func (s *ConsumerConfig) OnBackpressure(onBackpressure func(value interface{})) {
	s.onBackpressure = onBackpressure
}

func (b *BroadcasterConfig) PostBroadcast(postBroadcast func(interface{})) {
	b.postBroadcast = postBroadcast
}
