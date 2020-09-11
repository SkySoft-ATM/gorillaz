package stream

import (
	"context"
	"time"
)

type Event struct {
	Ctx        context.Context
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

// CtxWithDeadline returns the event context with deadline applied
func CtxWithDeadline(e *Event) (context.Context, context.CancelFunc) {
	var cancel context.CancelFunc
	v := e.Ctx.Value(deadlineKey)
	if v == nil {
		return e.Ctx, cancel
	}
	deadline := v.(int64)
	sec := deadline / 1000000000
	ns := deadline - sec*1000000000

	t := time.Unix(sec, ns)
	return context.WithDeadline(e.Ctx, t)
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

// EventDeadline returns the event deadline as a unix timestamp in ns if available
func EventDeadline(evt *Event) (int64, bool) {
	v := evt.Ctx.Value(deadlineKey)
	if v == nil {
		return 0, false
	}
	ts := v.(int64)
	if ts == 0 {
		return 0, false
	}
	return ts, true
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

func SetEventTypeStr(evt *Event, eventType string) {
	if evt.Ctx == nil {
		evt.Ctx = context.Background()
	}
	evt.Ctx = context.WithValue(evt.Ctx, eventTypeKey, eventType)
}

func EventTypeStr(evt *Event) string {
	if evt.Ctx == nil {
		return ""
	}
	v := evt.Ctx.Value(eventTypeKey)
	if v == nil {
		return ""
	}
	if eType, ok := v.(string); ok {
		return eType
	}
	return ""
}

func SetEventTypeVersionStr(evt *Event, version string) {
	if evt.Ctx == nil {
		evt.Ctx = context.Background()
	}
	evt.Ctx = context.WithValue(evt.Ctx, eventTypeVersionKey, version)
}

func EventTypeVersionStr(evt *Event) string {
	if evt.Ctx == nil {
		return ""
	}
	v := evt.Ctx.Value(eventTypeVersionKey)
	if v == nil {
		return ""
	}
	if eType, ok := v.(string); ok {
		return eType
	}
	return ""
}
