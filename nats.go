package gorillaz

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/opentracing/opentracing-go"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"time"
)

// PullNatsStream returns the next subject and event for the given stream and consumer
// If the event is processed successfully, it must be acknowledge to get a new message. If the message is not acknowledge, then PullNatsStream will return the same event multiple times
// If no message is available after the ctx timeout, then an error nats.ErrTimeout is returned with an empty subject and an event nil
func (g *Gaz) PullNatsStream(ctx context.Context, stream string, consumer string) (subject string, event *stream.Event, err error) {
	subj := "$JS.API.CONSUMER.MSG.NEXT." + stream + "." + consumer
	msg, err := g.NatsConn.RequestWithContext(ctx, subj, nil)
	if err != nil {
		return "", nil, err
	}
	e := msgToEvent(msg)
	e.AckFunc = func() error {
		return msg.Respond(nil)
	}
	return msg.Subject, e, nil
}

// MsgHandler handles received events from Nats
// If NatsConsumerOpts.AutoAck is set, if MsgHandler returns no error, the message will be acknowledged. If an error is returned, the event won't be acknowledged.
type MsgHandler func(subject string, event *stream.Event) (reply *stream.Event, err error)

type NatsConsumerOpts struct {
	AutoAck bool
	Queue   string
}

type NatsConsumerOpt func(n *NatsConsumerOpts)

// WithAutoAck automatic acknowledge message received if MsgHandle returns no error
func WithAutoAck() NatsConsumerOpt {
	return func(o *NatsConsumerOpts) {
		o.AutoAck = true
	}
}

// WithQueue configures Nats Queue consumer
func WithQueue(queue string) NatsConsumerOpt {
	return func(o *NatsConsumerOpts) {
		o.Queue = queue
	}
}

// SubscribeNatsSubject subscribes to a Nats stream, and forward received messages to handler
// An error is returned if the subscription fails, but not when the connection with Nats is interrupted
func (g *Gaz) SubscribeNatsSubject(subject string, handler MsgHandler, opts ...NatsConsumerOpt) (*NatsSubscription, error) {
	c := &NatsConsumerOpts{
		AutoAck: false,
	}
	for _, o := range opts {
		o(c)
	}
	if g.NatsConn == nil {
		return nil, fmt.Errorf("gorillaz nats connection is nil, cannot consume stream")
	}

	do := func(m *nats.Msg) {
		e := msgToEvent(m)

		if !c.AutoAck && m.Reply != "" {
			e.AckFunc = func() error {
				return m.Respond(nil)
			}
		}

		reply, err := handler(m.Subject, e)
		if err == nil {
			if m.Reply != "" && c.AutoAck {
				Log.Debug("ack", zap.String("subject", subject), zap.String("reply", m.Reply))
				if err := m.Respond(nil); err != nil {
					// TODO: not great for consumer, he may receive the same event multiple times and really be aware
					Log.Error("failed to ack event", zap.Error(err))
				}
				return
			}
		}

		if reply != nil && m.Reply != "" {
			// serialize the reply
			if reply.Ctx == nil {
				reply.Ctx = context.Background()
			}
			if opentracing.SpanFromContext(reply.Ctx) == nil {
				// check if there is no span in the original msg
				originalSpan := opentracing.SpanFromContext(e.Ctx)
				if originalSpan != nil {
					reply.Ctx = opentracing.ContextWithSpan(reply.Ctx, originalSpan)
				}
			}

			metadata := &stream.Metadata{}
			err := stream.ContextToMetadata(reply.Ctx, metadata, subject, false)
			if err != nil {
				Log.Error("failed to create metadata from context", zap.Error(err))
			}

			r := &stream.StreamEvent{Metadata: metadata, Key: reply.Key, Value: reply.Value}
			b, err := proto.Marshal(r)
			if err != nil {
				Log.Error("failed to marshal response", zap.Error(err))
				return
			}

			Log.Debug("reply", zap.String("subject", subject), zap.String("reply", m.Reply))
			if err := m.Respond(b); err != nil {
				Log.Error("failed to ack event", zap.Error(err))
			}
		}
	}

	var err error
	var sub *nats.Subscription

	if c.Queue == "" {
		sub, err = g.NatsConn.Subscribe(subject, func(m *nats.Msg) {
			do(m)
		})
	} else {
		sub, err = g.NatsConn.QueueSubscribe(subject, c.Queue, func(m *nats.Msg) {
			do(m)
		})
	}

	if err == nil {
		return &NatsSubscription{n: sub}, nil
	}
	return nil, err
}

type NatsPublishOpts struct {
	tracingEnabled bool
}

type NatsPublishOpt func(opts *NatsPublishOpts)

func WithNatsTracingEnabled() NatsPublishOpt {
	return func(o *NatsPublishOpts) {
		o.tracingEnabled = true
	}
}

func (g *Gaz) NatsPublish(subject string, e *stream.Event, opts ...NatsPublishOpt) error {
	var metadata stream.Metadata
	conf := &NatsPublishOpts{}

	for _, opt := range opts {
		opt(conf)
	}
	err := stream.ContextToMetadata(e.Ctx, &metadata, subject, conf.tracingEnabled)
	if err != nil {
		return err
	}
	evt := stream.StreamEvent{Key: e.Key, Value: e.Value, Metadata: &metadata}
	b, err := proto.Marshal(&evt)
	if err != nil {
		return err
	}
	return g.NatsConn.Publish(subject, b)
}

func (g *Gaz) NatsRequest(ctx context.Context, subject string, e *stream.Event, opts ...NatsPublishOpt) (*stream.Event, error) {
	var metadata stream.Metadata
	conf := &NatsPublishOpts{}

	for _, opt := range opts {
		opt(conf)
	}
	err := stream.ContextToMetadata(e.Ctx, &metadata, subject, conf.tracingEnabled)
	if err != nil {
		return nil, err
	}
	evt := stream.StreamEvent{Key: e.Key, Value: e.Value, Metadata: &metadata}
	b, err := proto.Marshal(&evt)
	if err != nil {
		return nil, err
	}
	msg, err := g.NatsConn.RequestWithContext(ctx, subject, b)
	if err != nil {
		return nil, err
	}

	// try to unmarshal msg into stream.Event
	event := &stream.StreamEvent{}
	err = proto.Unmarshal(msg.Data, event)
	if err != nil {
		// if may not really be a StreamEvent, fallback
		return &stream.Event{Value: msg.Data}, nil
	}

	eCtx := stream.MetadataToContext(event.Metadata)
	return &stream.Event{Ctx: eCtx, Key: e.Key, Value: e.Value}, nil
}

func msgToEvent(msg *nats.Msg) *stream.Event {
	var evt stream.StreamEvent
	var e stream.Event
	// try to deserialize object
	err := proto.Unmarshal(msg.Data, &evt)
	if err != nil {
		// that may not be a StreamEvent
		// take the raw payload as it comes
		e = stream.Event{Ctx: context.Background(), Value: msg.Data}
	} else {
		e = stream.Event{Ctx: stream.MetadataToContext(evt.Metadata), Key: evt.Key, Value: evt.Value}
	}
	return &e
}

type NatsSubscription struct {
	n *nats.Subscription
}

func (n *NatsSubscription) Unsubscribe() error {
	return n.n.Unsubscribe()
}

func (n *NatsSubscription) Subject() string {
	return n.n.Subject
}

func (n *NatsSubscription) Queue() string {
	return n.n.Queue
}

// mustInitNats connects to nats broker with address addr, or panic
// if successful, g.NatsConn is set
func (g *Gaz) mustInitNats(addr string) {
	timeout := time.Duration(g.Viper.GetUint64("nats.connect_timeout_ms")) * time.Millisecond
	var err error
	g.NatsConn, err = nats.Connect(addr, nats.Timeout(timeout))
	if err != nil {
		Log.Panic("failed to initialize nats connection", zap.Error(err))
	}
}
