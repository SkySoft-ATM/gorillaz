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
