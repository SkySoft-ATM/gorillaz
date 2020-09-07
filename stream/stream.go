package stream

import (
	"context"
	"time"
)

type Event struct {
	ctx        context.Context
	Key, Value []byte
	AckFunc    func() error
}

func (e *Event) Ack() error {
	if e.AckFunc == nil {
		return nil
	}
	return e.AckFunc()
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type key string

const eventTypeKey = key("event_type")
const eventTypeVersionKey = key("event_type_version")
const eventTimeNs = key("event_time_ns")
const streamTimestampNs = key("stream_timestamp_ns")
const originStreamTimestampNs = key("origin_stream_timestamp_ns")
const deadlineKey = key("deadline")

// StreamTimestamp returns the time when the event was sent from the producer in Epoch in nanoseconds
func StreamTimestamp(e *Event) int64 {
	if e.ctx == nil {
		return 0
	}
	ts := e.ctx.Value(streamTimestampNs)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}

// EventCtx returns the context with cancel function for a given event
func EventCtx(e *Event) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	v := e.ctx.Value(deadlineKey)
	if v == nil {
		return e.ctx, cancel
	}
	deadline := v.(int64)
	sec := deadline / 1000000000
	ns := deadline - sec*1000000000

	t := time.Unix(sec, ns)
	return context.WithDeadline(e.ctx, t)
}

// OriginStreamTimestamp returns the time when the event was sent from the first producer in Epoch in nanoseconds
func OriginStreamTimestamp(e *Event) int64 {
	if e.ctx == nil {
		return 0
	}
	ts := e.ctx.Value(originStreamTimestampNs)
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
	if evt.ctx == nil {
		evt.ctx = context.Background()
	}
	evt.ctx = context.WithValue(evt.ctx, eventTimeNs, t.UnixNano())
}

// EventTimestamp returns the event creation time Epoch in nanoseconds
func EventTimestamp(e *Event) int64 {
	if e.ctx == nil {
		return 0
	}
	ts := e.ctx.Value(eventTimeNs)
	if ts != nil {
		if res, ok := ts.(int64); ok {
			return res
		}
	}
	return 0
}

func SetEventTypeStr(evt *Event, eventType string) {
	if evt.ctx == nil {
		evt.ctx = context.Background()
	}
	evt.ctx = context.WithValue(evt.ctx, eventTypeKey, eventType)
}

func EventTypeStr(evt *Event) string {
	if evt.ctx == nil {
		return ""
	}
	v := evt.ctx.Value(eventTypeKey)
	if v == nil {
		return ""
	}
	if eType, ok := v.(string); ok {
		return eType
	}
	return ""
}

func SetEventTypeVersionStr(evt *Event, version string) {
	if evt.ctx == nil {
		evt.ctx = context.Background()
	}
	evt.ctx = context.WithValue(evt.ctx, eventTypeVersionKey, version)
}

func EventTypeVersionStr(evt *Event) string {
	if evt.ctx == nil {
		return ""
	}
	v := evt.ctx.Value(eventTypeVersionKey)
	if v == nil {
		return ""
	}
	if eType, ok := v.(string); ok {
		return eType
	}
	return ""
}
