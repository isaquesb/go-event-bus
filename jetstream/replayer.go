package jetstream

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

func NewReplayer(js jetstream.JetStream) *Replayer {
	return &Replayer{js: js}
}

type Replayer struct {
	js jetstream.JetStream
}
type ReplayOptions struct {
	FromStream string
	ToSubject  string
	Limit      int
}

func (r *Replayer) Replay(
	ctx context.Context,
	opts ReplayOptions,
) error {
	c, err := r.js.CreateOrUpdateConsumer(ctx, opts.FromStream, jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return err
	}

	count := 0

	for {
		if opts.Limit > 0 && count >= opts.Limit {
			break
		}

		msg, err := c.Next()
		if err != nil {
			break
		}

		_, err = r.js.Publish(ctx, opts.ToSubject, msg.Data())
		if err != nil {
			_ = msg.Nak()
			continue
		}

		_ = msg.Ack()
		count++
	}

	return nil
}

/*
replayer.Replay(ctx, ReplayOptions{
	FromStream: "EVENTS_DLQ",
	ToSubject:  "chat.created",
	Limit:      100,
})
*/
