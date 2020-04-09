package stream

import (
	"context"
	"time"
)

type Event struct {
	Ctx        context.Context
	Key, Value []byte
	Ack        func()
}

type Publisher interface {
	Subscribe(context.Context, Subscriber, ...SubscriptionOption)
}

type Subscriber interface {
	OnNext(*Event) error
	OnError(error) // called at most once
	OnComplete()   // called at most once if the subscription is not canceled before the stream completes
}

type SubscriptionOption func(options SubscriptionConfig)

type subscriber struct {
	onNext     func(*Event) error
	onError    func(error)
	onComplete func()
}

func (s subscriber) OnNext(event *Event) error {
	return s.onNext(event)
}

func (s subscriber) OnError(err error) {
	s.onError(err)
}

func (s subscriber) OnComplete() {
	s.onComplete()
}

func CreateSubscriber(onNext func(*Event) error, onError func(error), onComplete func()) Subscriber {
	return subscriber{
		onNext:     onNext,
		onError:    onError,
		onComplete: onComplete,
	}
}

// unfortunately we need this to be an interface so that we can extend it for transport specific configuration
type SubscriptionConfig interface {
	AutoAck() bool
	ReconnectOnError() bool
	OnError() func(streamName string, err error)
	ReconnectOnComplete() bool
	OnComplete() func(streamName string)
	OnConnected() func(streamName string)
	OnDisconnected() func(streamName string)

	SetAutoAck(bool)
	SetReconnectOnError(bool)
	SetOnError(func(streamName string, err error))
	SetReconnectOnComplete(bool)
	SetOnComplete(func(streamName string))
	SetOnConnected(func(streamName string))
	SetOnDisconnected(func(streamName string))
}

type subConfig struct {
	autoAck             bool
	reconnectOnError    bool
	onError             func(streamName string, err error)
	reconnectOnComplete bool
	onComplete          func(streamName string)
	onConnected         func(streamName string)
	onDisconnected      func(streamName string)
}

func (s *subConfig) AutoAck() bool {
	return s.autoAck
}

func (s *subConfig) ReconnectOnError() bool {
	return s.reconnectOnError
}

func (s *subConfig) OnError() func(streamName string, err error) {
	return s.onError
}

func (s *subConfig) ReconnectOnComplete() bool {
	return s.reconnectOnComplete
}

func (s subConfig) OnComplete() func(streamName string) {
	return s.onComplete
}

func (s *subConfig) OnConnected() func(streamName string) {
	return s.onConnected
}

func (s *subConfig) OnDisconnected() func(streamName string) {
	return s.onDisconnected
}

func (s subConfig) SetAutoAck(b bool) {
	s.autoAck = b
}

func (s *subConfig) SetReconnectOnError(b bool) {
	s.reconnectOnError = b
}

func (s *subConfig) SetOnError(f func(streamName string, err error)) {
	s.onError = f
}

func (s *subConfig) SetReconnectOnComplete(b bool) {
	s.reconnectOnComplete = b
}

func (s *subConfig) SetOnComplete(f func(streamName string)) {
	s.onComplete = f
}

func (s *subConfig) SetOnConnected(f func(streamName string)) {
	s.onConnected = f
}

func (s *subConfig) SetOnDisconnected(f func(streamName string)) {
	s.onDisconnected = f
}

func DefaultSubscriptionOptions() SubscriptionConfig {
	return &subConfig{
		autoAck:          true,
		reconnectOnError: false,
		onError: func(streamName string, err error) {

		},
		reconnectOnComplete: false,
		onComplete: func(streamName string) {

		},
	}
}

func WithManualAck() SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetAutoAck(false)
	}
}

func ReconnectOnError() SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetReconnectOnError(true)
	}
}

func OnError(f func(string, error)) SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetOnError(f)
	}
}

func ReconnectOnComplete() SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetReconnectOnComplete(true)
	}
}

func OnComplete(f func(string)) SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetOnComplete(f)
	}
}

func OnConnected(f func(string)) SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetOnConnected(f)
	}
}

func OnDisconnected(f func(string)) SubscriptionOption {
	return func(s SubscriptionConfig) {
		s.SetOnDisconnected(f)
	}
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type key string

const eventTimeNs = key("event_time_ns")
const streamTimestampNs = key("stream_timestamp_ns")
const originStreamTimestampNs = key("origin_stream_timestamp_ns")

// StreamTimestamp returns the time when the event was sent from the producer in Epoch in nanoseconds
func StreamTimestamp(e *Event) int64 {
	if e.Ctx == nil {
		return 0
	}
	ts := e.Ctx.Value(streamTimestampNs)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}

// OriginStreamTimestamp returns the time when the event was sent from the first producer in Epoch in nanoseconds
func OriginStreamTimestamp(e *Event) int64 {
	if e.Ctx == nil {
		return 0
	}
	ts := e.Ctx.Value(originStreamTimestampNs)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}

// SetEventTimestamp stores in the event the timestamp of when the event happened (as opposite as when it was streamed).
// use this function to store values such as when an observation was recorded
func SetEventTimestamp(evt *Event, t time.Time) {
	if evt.Ctx == nil {
		evt.Ctx = context.Background()
	}
	evt.Ctx = context.WithValue(evt.Ctx, eventTimeNs, t.UnixNano())
}

// EventTimestamp returns the event creation time Epoch in nanoseconds
func EventTimestamp(e *Event) int64 {
	if e.Ctx == nil {
		return 0
	}
	ts := e.Ctx.Value(eventTimeNs)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}
