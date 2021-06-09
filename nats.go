package gorillaz

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/skysoft-atm/gorillaz/stream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Deprecated: use github.com/nats.io/nats.go/Jetstream instead with NatsMsgToEvent
// PullJetstream returns the next subject and event for the given stream and consumer
// If the event is processed successfully, it must be acknowledge to get a new message. If the message is not acknowledge, then PullJetstream will return the same event multiple times
// If no message is available after the ctx timeout, then an error nats.ErrTimeout is returned with an empty subject and an event nil
func (g *Gaz) PullJetstream(ctx context.Context, stream string, consumer string) (subject string, event *stream.Event, err error) {
	evtC, errC := g.PullJetstreamBatch(ctx, stream, consumer, CloseOnEndOfStream(true))
	select {
	case err := <-errC:
		return "", nil, err
	case evt := <-evtC:
		return evt.Subject(), evt, nil
	}
}

type JSApiConsumerGetNextRequest struct {
	Expires time.Duration `json:"expires,omitempty"`
	Batch   int           `json:"batch,omitempty"`
	NoWait  bool          `json:"no_wait,omitempty"`
}

type pullOptions struct {
	batchSize                 int
	closeOnEndOfStreamReached bool
	ackImmediately            bool
}

type PullOption func(*pullOptions)

func CloseOnEndOfStream(c bool) func(o *pullOptions) {
	return func(o *pullOptions) {
		o.closeOnEndOfStreamReached = c
	}
}

func AckImmediately(c bool) func(o *pullOptions) {
	return func(o *pullOptions) {
		o.ackImmediately = c
	}
}

func BatchSize(s int) func(o *pullOptions) {
	return func(o *pullOptions) {
		o.batchSize = s
	}
}

func (g *Gaz) AddStreamEnvIfMissing(streamName string) string {
	if !strings.HasPrefix(streamName, g.Env) {
		return g.Env + "-" + streamName
	}
	return streamName
}

func (g *Gaz) AddConsumerEnvIfMissing(consumerName string) string {
	if !strings.HasPrefix(consumerName, g.Env) {
		return g.Env + consumerName
	}
	return consumerName
}

func nextMsgSubject(stream, consumer string) string {
	return "$JS.API.CONSUMER.MSG.NEXT." + stream + "." + consumer
}

// Deprecated: use github.com/nats.io/nats.go/Jetstream instead with NatsMsgToEvent
// Pulls messages from a stream by batch, the batch size is configurable
// A consumer with the given name must exists before calling this method.
func (g *Gaz) PullJetstreamBatch(ctx context.Context, streamName string, consumer string, options ...PullOption) (<-chan *stream.Event, <-chan error) {

	streamName = g.AddStreamEnvIfMissing(streamName)
	consumer = g.AddConsumerEnvIfMissing(consumer)

	o := pullOptions{
		batchSize:                 100,
		closeOnEndOfStreamReached: false,
		ackImmediately:            false,
	}

	for _, opt := range options {
		opt(&o)
	}

	eventChan := make(chan *stream.Event, o.batchSize)
	errChan := make(chan error, 1)

	req := JSApiConsumerGetNextRequest{Batch: o.batchSize, Expires: 3 * time.Second}
	reqB, err := json.Marshal(req)
	if err != nil {
		errChan <- fmt.Errorf("failed to marshal batch request, %+v", err)
		close(errChan)
		close(eventChan)
		return eventChan, errChan
	}

	go func() {
		defer func() {
			close(eventChan)
			close(errChan)
		}()

		sub, err := g.NatsConn.SubscribeSync(nats.NewInbox())
		if err != nil {
			errChan <- fmt.Errorf("failed to subscribe to inbox, %+v", err)
			return
		}
		subject := nextMsgSubject(streamName, consumer)

		defer func() {
			if err := sub.Unsubscribe(); err != nil {
				Log.Warn("failed to unsubscribe subscriber", zap.Error(err))
			}
		}()

		for {
			if ctx.Err() != nil {
				errChan <- ctx.Err()
				return
			}
			err = g.NatsConn.PublishMsg(&nats.Msg{Subject: subject, Reply: sub.Subject, Data: reqB})
			if err != nil {
				errChan <- fmt.Errorf("failed to create jetstream context, %+v", err)
				return
			}
			for i := 0; i < o.batchSize; i++ {
				msg, err := sub.NextMsgWithContext(ctx)
				if err != nil {
					errChan <- fmt.Errorf("failed to load next message, %+v", err)
					return
				}

				// when we requested more messages than available, we receive a 408 status code without a reply
				if msg.Reply == "" {
					// that's not a jetstream message, probably a status message
					continue
				}

				if o.ackImmediately {
					if err := msg.Ack(); err != nil {
						errChan <- fmt.Errorf("failed to ack received msg, %+v", err)
						return
					}
				}

				evt := NatsMsgToEvent(msg)
				evt.AckFunc = func() error { return msg.Ack() }
				eventChan <- evt

				if evt.Pending() == 0 && o.closeOnEndOfStreamReached {
					return
				}
			}
		}
	}()

	return eventChan, errChan
}

// MsgHandler handles received events from Nats
// If NatsConsumerOpts.AutoAck is set, if MsgHandler returns no error, the message will be acknowledged. If an error is returned, the event won't be acknowledged.
type MsgHandler func(subject string, event *stream.Event) (reply *stream.Event, err error)

type NatsConsumerOpts struct {
	autoAck        bool
	tracingEnabled bool
	queue          string
}

type NatsConsumerOpt func(n *NatsConsumerOpts)

// WithAutoAck automatic acknowledge message received if MsgHandle returns no error
func WithAutoAck() NatsConsumerOpt {
	return func(o *NatsConsumerOpts) {
		o.autoAck = true
	}
}

func WithTracing() NatsPublishOpt {
	return func(o *NatsPublishOpts) {
		o.tracingEnabled = true
	}
}

// WithQueue configures Nats Queue consumer
func WithQueue(queue string) NatsConsumerOpt {
	return func(o *NatsConsumerOpts) {
		o.queue = queue
	}
}

// SubscribeNatsSubject subscribes to a Nats stream, and forward received messages to handler
// An error is returned if the subscription fails, but not when the connection with Nats is interrupted
func (g *Gaz) SubscribeNatsSubject(subject string, handler MsgHandler, opts ...NatsConsumerOpt) (*NatsSubscription, error) {
	if g.addEnvPrefixToNats {
		subject = g.Env + "." + subject
	}
	c := &NatsConsumerOpts{
		autoAck:        false,
		tracingEnabled: false,
	}
	for _, o := range opts {
		o(c)
	}
	if g.NatsConn == nil {
		return nil, fmt.Errorf("gorillaz nats connection is nil, cannot consume stream")
	}

	do := func(m *nats.Msg) {
		e := NatsMsgToEvent(m)

		// if there is no auto ack, then the user is responsible for calling event.Ack
		if !c.autoAck && m.Reply != "" {
			e.AckFunc = func() error {
				return m.Respond(nil)
			}
		}

		response, err := handler(m.Subject, e)

		if err == nil {
			if m.Reply != "" && c.autoAck {
				Log.Debug("ack", zap.String("subject", subject), zap.String("reply", m.Reply))
				if err := m.Respond(nil); err != nil {
					// TODO: not great for consumer, he may receive the same event multiple times and really be aware
					Log.Error("failed to ack event", zap.Error(err))
				}
				return
			}
		}

		if response != nil && m.Reply != "" {
			if response.Ctx == nil {
				response.Ctx = context.Background()
			}
			stream.FillTracingSpan(response, e)

			metadata, err := stream.EventMetadata(response)
			if err != nil {
				Log.Error("failed to create metadata from event", zap.Error(err))
			}

			r := &stream.StreamEvent{Metadata: metadata, Key: response.Key, Value: response.Value}
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

	if c.queue == "" {
		sub, err = g.NatsConn.Subscribe(subject, func(m *nats.Msg) {
			do(m)
		})
	} else {
		sub, err = g.NatsConn.QueueSubscribe(subject, c.queue, func(m *nats.Msg) {
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
	if g.addEnvPrefixToNats {
		subject = g.Env + "." + subject
	}
	conf := &NatsPublishOpts{}

	for _, opt := range opts {
		opt(conf)
	}
	metadata, err := stream.EventMetadata(e)
	if err != nil {
		return err
	}
	evt := stream.StreamEvent{Key: e.Key, Value: e.Value, Metadata: metadata}
	b, err := proto.Marshal(&evt)
	if err != nil {
		return err
	}
	return g.NatsConn.Publish(subject, b)
}

func (g *Gaz) NatsRequest(ctx context.Context, subject string, e *stream.Event, opts ...NatsPublishOpt) (*stream.Event, error) {
	if g.addEnvPrefixToNats {
		subject = g.Env + "." + subject
	}
	conf := &NatsPublishOpts{}

	for _, opt := range opts {
		opt(conf)
	}
	metadata, err := stream.EventMetadata(e)
	if err != nil {
		return nil, err
	}
	evt := stream.StreamEvent{Key: e.Key, Value: e.Value, Metadata: metadata}
	b, err := proto.Marshal(&evt)
	if err != nil {
		return nil, err
	}
	msg, err := g.NatsConn.RequestWithContext(ctx, subject, b)
	if err != nil {
		return nil, err
	}
	return NatsMsgToEvent(msg), nil
}

func NatsMsgToEvent(msg *nats.Msg) *stream.Event {
	var evt stream.StreamEvent
	value := msg.Data
	var key []byte
	ctx := context.Background()

	// try to deserialize object
	err := proto.Unmarshal(msg.Data, &evt)
	if err == nil {
		key = evt.Key
		value = evt.Value
		ctx = stream.Ctx(evt.Metadata)
	}
	e := &stream.Event{Ctx: ctx, Key: key, Value: value, AckFunc: func() error { return nil }}
	meta, err := msg.Metadata()

	if err == nil && meta != nil {
		e.SetPending(int(meta.NumPending))
		e.SetConsumerSeq(int(meta.Sequence.Consumer))
		e.SetStreamSeq(int(meta.Sequence.Stream))
		e.SetSubject(msg.Subject)
		e.SetStream(meta.Stream)
	}
	return e
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

	var opts []nats.Option
	opts = append(opts, nats.Timeout(timeout))

	rootCAs := g.Viper.GetString("nats.ca")
	if len(rootCAs) > 0 {
		cas := strings.Split(rootCAs, ",")
		opts = append(opts, nats.RootCAs(cas...))
	}

	crt := g.Viper.GetString("nats.crt")
	key := g.Viper.GetString("nats.key")

	if len(crt) > 0 || len(key) > 0 {
		opts = append(opts, nats.ClientCert(crt, key))
	}

	g.NatsConn, err = nats.Connect(addr, opts...)
	if err != nil {
		Log.Panic("failed to initialize nats connection", zap.Error(err))
	}
}
