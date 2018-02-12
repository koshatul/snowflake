package snowflakeamqp

import (
	"context"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

// Locker is an AMQP-based implementation of snowflake.Locker.
type Locker struct {
	DSN string

	once     sync.Once
	broker   *amqp.Connection
	closed   chan *amqp.Error
	requests chan chan<- *amqp.Connection
}

// Do calls fn while holding a lock on the resource named r.
//
// It blocks until the lock is acquired and fn returns, or until ctx is
// canceled.
//
// c, the context passed to fn, is derived from ctx. If the lock is 'lost'
// before fn returns, c is canceled.
func (l *Locker) Do(ctx context.Context, r string, fn func(c context.Context)) error {
	l.once.Do(l.init)

	reply := make(chan *amqp.Connection, 1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.requests <- reply:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case broker := <-reply:
		m := &fsm{
			broker: broker,
			ctx:    ctx,
			res:    r,
			fn:     fn,
		}
		return m.run()
	}
}

// Run starts the locker, which mantains a persistent connection to the AMQP
// broker.
func (l *Locker) Run(ctx context.Context) error {
	l.once.Do(l.init)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reply := <-l.requests:
			l.dial(ctx)
			reply <- l.broker // reply queue is buffered
		case err := <-l.closed:
			fmt.Println(err)
		}
	}
}

func (l *Locker) init() {
	l.requests = make(chan chan<- *amqp.Connection)
}

func (l *Locker) dial(ctx context.Context) {
	fmt.Println("DIALING")

	if l.broker != nil {
		return
	}

	dsn := l.DSN
	if dsn == "" {
		dsn = "amqp://localhost"
	}

	broker, err := amqp.Dial(dsn) // TODO: ctx dialer
	if err != nil {
		fmt.Println(err)
		return
	}

	l.closed = make(chan *amqp.Error)
	broker.NotifyClose(l.closed)
	l.broker = broker
}
