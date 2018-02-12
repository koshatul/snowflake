package snowflakeamqp

import (
	"context"

	"github.com/streadway/amqp"
)

// Locker is an AMQP-based implementation of snowflake.Locker.
type Locker struct {
	Broker *amqp.Connection
}

// Do calls fn while holding a lock on the resource named r.
//
// It blocks until the lock is acquired and fn returns, or until ctx is
// cancelled.
//
// c, the context passed to fn, is derived from ctx and is canceled when
// the lock is unexpedly released before fn returns, such as when
// communications with the lock coordinator are severed.
func (l *Locker) Do(ctx context.Context, r string, fn func(c context.Context) error) error {
	m := &fsm{
		broker: l.Broker,
		ctx:    ctx,
		res:    r,
		fn:     fn,
	}

	return m.run()
}
