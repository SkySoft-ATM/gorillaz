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
	name    string
	channel chan<- interface{}
}

type ttlValue struct {
	expiresAt time.Time
	value     interface{}
}

type BroadcasterConfig struct {
	onBackpressure func(consumerName string, value interface{})
	postBroadcast  func(interface{})
}

type BroadcasterOptionFunc func(*BroadcasterConfig) error

func (b *BroadcasterConfig) OnBackpressure(onBackpressure func(consumerName string, value interface{})) {
	b.onBackpressure = onBackpressure
}

func (b *BroadcasterConfig) PostBroadcast(postBroadcast func(interface{})) {
	b.postBroadcast = postBroadcast
}
