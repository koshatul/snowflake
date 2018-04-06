package snowflakeamqp

import (
	"context"

	"github.com/streadway/amqp"
	"go.uber.org/multierr"
)

type state func() (state, error)

type fsm struct {
	// broker is the AMQP broker connection used to establish new AMQP channels
	broker *amqp.Connection

	// ctx is the context that was passed to Locker.Do().
	// Lock acquisition is aborted if ctx is canceled.
	ctx context.Context

	// res is the name of the resource being locked.
	res string

	// fn is the "work function" to be executed when the lock is acquired.
	fn func(context.Context)

	// channel is the AMQP channel used to consume "tokens" which indicate the
	// availability of the resource.
	channel *amqp.Channel

	// closed is closed when channel is closed.
	closed chan *amqp.Error

	// tokens is the channel on which incoming token messages are delivered.
	tokens <-chan amqp.Delivery

	// token is the token message which is held for the duration of the lock.
	// It is never acknowledged, allowing it to be redelivered to another
	// consumer when fn() panics or returns.
	token *amqp.Delivery

	// lockChannel is the AMQP channel that is the exclusive consumer of the
	// locking queue. It may be nil if the lock was never acquired.
	lockChannel *amqp.Channel
}

// execute runs the state machine.
func (m *fsm) execute() (err error) {
	defer func() {
		err = m.shutdown(err)
	}()

	m.channel, err = m.broker.Channel()
	if err != nil {
		return
	}

	m.closed = make(chan *amqp.Error, 1)
	m.channel.NotifyClose(m.closed)

	err = m.consume()
	if err != nil {
		return
	}

	state := m.wait
	for state != nil && err == nil {
		state, err = state()
	}

	return
}

// shutdown is called when the state machine stops executing.
// It is passed the error from the last executed state, which may be nil.
func (m *fsm) shutdown(err error) error {
	if m.channel != nil {
		err = multierr.Append(
			err,
			m.channel.Close(),
		)
	}

	if m.lockChannel != nil {
		err = multierr.Append(
			err,
			m.lockChannel.Close(),
		)
	}

	return err
}

// wait is the initial state. It waits for an "availability token" to be
// delivered then transitions to the m.acquire state.
func (m *fsm) wait() (state, error) {
	select {
	case <-m.ctx.Done():
		// If the context is canceled before we acquire the lock, we simply
		// shutdown.
		return nil, m.ctx.Err()

	case tok, ok := <-m.tokens:
		if !ok {
			// m.tokens is closed which means the consumer has stopped.
			// This is always unexpected and so must be caused by the
			// channel closing.
			return m.waitClose, nil
		}

		// If we receive a token, it's highly likely that the resource has
		// become available.
		m.token = &tok
		return m.acquire, nil
	}
}

// acquire is a state that attempts to start an exclusive consumer on the
// on the lock queue. It expects that m.token points to a valid unacknowledged
// token message.
func (m *fsm) acquire() (state, error) {
	c, err := m.broker.Channel()
	if err != nil {
		return nil, err
	}

	queue := lockQueue(m.res)
	_, err = c.QueueDeclare(
		queue,
		false, // durable,
		true,  // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return nil, err
	}

	// Attempt to start an exclusive consumer on the lock channel.
	// There should never be any messages delivered to this queue.
	_, err = c.Consume(
		queue,
		m.res,
		true,  // autoAck
		true,  // exclusive
		false, // noLocal
		false, // noWait bool
		nil,   // args
	)

	if err != nil {
		if amqpErr, ok := err.(*amqp.Error); ok {
			if amqpErr.Code == amqp.ResourceLocked {
				// If the error is an AMQP "resource locked" exception, that
				// means another consumer already has exclusive access to the
				// lock queue. This could be because multiple tokens have been
				// published to the token queue, which can occur when two
				// competing machines start consuming at the same time.
				//
				// Go back to waiting if we can cleanly return the token to the
				// queue.
				return m.wait, m.token.Reject(true)
			}
		}

		return nil, err
	}

	m.lockChannel = c
	return m.invoke, nil
}

// invoke is a state that invokes the work function.
func (m *fsm) invoke() (state, error) {
	// ctx is passed to fn. It is canceled when the invoke state is exited.
	ctx, cancel := context.WithCancel(m.ctx)
	done := make(chan struct{})

	// run the work function in its own goroutine
	go func() {
		defer close(done)
		m.fn(ctx)
	}()

	defer func() {
		cancel() // if we exit this state for any reason cancel fn's ctx.
		<-done   // always block until fn returns
	}()

	for {
		select {
		case <-m.ctx.Done():
			// If the parent ctx is canceled while fn is executing, we exit and
			// rely on the defer above to until block until fn actually responds
			// to the cancelation by returning.
			return nil, m.ctx.Err()

		case <-done:
			// Done is closed when fn returns, we return the token to the queue
			// to notify other consumers.
			return nil, m.token.Reject(true)

		case tok, ok := <-m.tokens:
			if !ok {
				// m.tokens is closed which means the consumer has stopped.
				// This is always unexpected and so must be caused by the
				// channel closing.
				return m.waitClose, nil
			}

			// If we receive a token it must be a duplicate because the "real"
			// token is held in m.token, we ack them to remove them from the
			// queue permantently.
			if err := tok.Ack(false); err != nil { // false = single message
				return nil, err
			}
		}
	}
}

// waitClose is a state that waits for m.channel to close and returns the
// associated error. It is entered after any unexpected AMQP failure.
func (m *fsm) waitClose() (state, error) {
	select {
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case err := <-m.closed:
		return nil, err
	}
}

func (m *fsm) consume() error {
	queue := tokenQueue(m.res)

	q, err := m.channel.QueueDeclare(
		queue,
		false, // durable,
		true,  // auto-delete
		false, // exclusive
		false, // no-wait
		amqp.Table{
			// Only allow one undelivered message. Note that this isn't enough
			// on its own to prevent multiple messages appearing in the queues,
			// as the length does not include unacknowledged messages, but it
			// does eliminate some unnecessary network traffic.
			"x-max-length": int32(1),
		}, // args
	)
	if err != nil {
		return err
	}

	// If there are no existing consumers and no messages on the queue we
	// *MIGHT* be the first locker interested in this resource, publish a new
	// token message. Again, this just prevents some unnecessary network traffic.
	if q.Consumers == 0 && q.Messages == 0 {
		err = m.channel.Publish(
			"",
			queue,
			false, // mandatory
			false, // immediate
			amqp.Publishing{},
		)
		if err != nil {
			return err
		}
	}

	m.tokens, err = m.channel.Consume(
		queue,
		m.res, // consumer-tag
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait bool
		nil,   // args
	)

	return err
}

// lockQueue returns the name of the lock queue to use for the resource named r.
func lockQueue(r string) string {
	return r + ".lock"
}

// tokenQueue returns the name of the token queue to use for the resource named r.
func tokenQueue(r string) string {
	return r + ".tokens"
}
