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
	requests chan chan<- response
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

	reply := make(chan response, 1)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.requests <- reply:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case rsp := <-reply:
		if rsp.err != nil {
			return rsp.err
		}

		m := &fsm{
			broker: rsp.broker,
			ctx:    ctx,
			res:    r,
			fn:     fn,
		}

		return m.execute()
	}
}

type response struct {
	broker *amqp.Connection
	err    error
}

// Run starts the locker, which mantains a persistent connection to the AMQP
// broker.
func (l *Locker) Run(ctx context.Context) error {
	l.once.Do(l.init)

	defer func() {
		if l.broker != nil {
			l.broker.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reply := <-l.requests:
			err := l.dial(ctx)
			reply <- response{l.broker, err} // reply queue is buffered
		case err := <-l.closed:
			// TODO: log
			fmt.Println("unable to dial broker:", err)
		}
	}
}

func (l *Locker) init() {
	l.requests = make(chan chan<- response)
}

func (l *Locker) dial(ctx context.Context) error {
	if l.broker != nil {
		return nil
	}

	dsn := l.DSN
	if dsn == "" {
		dsn = "amqp://localhost"
	}

	broker, err := amqp.Dial(dsn) // TODO: ctx dialer
	if err != nil {
		return err
	}

	l.closed = make(chan *amqp.Error)
	broker.NotifyClose(l.closed)
	l.broker = broker

	return nil
}
