package stream

import (
	"context"
	"time"
)

type Event struct {
	Ctx        context.Context
	Key, Value []byte
}

const eventTimeNs = "event_time_ns"
const streamTimestampNs = "stream_timestamp_ns"
const originStreamTimestampNs = "origin_stream_timestamp_ns"

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
