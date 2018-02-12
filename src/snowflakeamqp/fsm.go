package snowflakeamqp

import (
	"context"
	"fmt"

	"github.com/streadway/amqp"
)

type state func() (state, error)

type fsm struct {
	broker *amqp.Connection
	ctx    context.Context
	res    string
	fn     func(context.Context) error

	lc, tc *amqp.Channel
	done   chan *amqp.Error
	tokens <-chan amqp.Delivery
	token  *amqp.Delivery
}

func (m *fsm) run() error {
	c, err := m.broker.Channel()
	if err != nil {
		return err
	}
	defer c.Close()

	m.tc = c

	err = m.declare()
	if err != nil {
		return err
	}

	m.done = make(chan *amqp.Error, 1)
	m.tc.NotifyClose(m.done)

	defer func() {
		if m.lc != nil {
			m.lc.Close()
		}
	}()

	state := m.wait
	for state != nil && err == nil {
		state, err = state()
	}

	return err
}

// wait is a state that waits for an "availability" token to be delivered.
func (m *fsm) wait() (state, error) {
	fmt.Println("ENTER STATE: wait")

	for {
		select {
		case <-m.ctx.Done():
			return nil, m.ctx.Err()

		case tok, ok := <-m.tokens:
			if ok {
				m.token = &tok
				return m.acquire, nil
			}

			// If m.tokens is closed before m.closed becomes readable
			// we don't want to attempt to read from it anymore, just wait
			// for the error to appear or the context to be canceled.
			m.tokens = nil

		case err := <-m.done:
			return nil, err
		}
	}
}

// acquire is a state that attempts to start an exclusive consumer on the
// on the lock queue. It assumes a token
func (m *fsm) acquire() (state, error) {
	fmt.Println("ENTER STATE: acquire")

	if m.token == nil {
		panic("the acquire state requires that a token has been received")
	}

	c, err := m.broker.Channel()
	if err != nil {
		return nil, err
	}

	// Attempt to start an exclusive consumer on the lock channel.
	// There should never be any messages delivered to this channel, but
	// noAck = true discards any that may be delivered here manually.
	_, err = c.Consume(
		m.res+".lock",
		m.res,
		true,  // autoAck
		true,  // exclusive
		false, // noLocal
		false, // noWait bool
		nil,   // args
	)

	// The consumer started successfully, we now hold the lock and can invoke
	// the work function.
	if err == nil {
		m.lc = c
		return m.invoke, nil
	}

	// If the error is an AMQP "resource locked" exception, that means another
	// consumer already has exclusive access to the lock queue. This could be
	// just be a timing issue, or related to multiple tokens being delivered.
	// Either way, we simply return the token to the queue and try again.
	if amqpErr, ok := err.(*amqp.Error); ok {
		if amqpErr.Code == amqp.ResourceLocked {
			err = m.token.Reject(true) // true = requeue
			if err == nil {
				return m.wait, nil
			}
		}
	}

	return nil, err
}

// invoke is a state that invokes the work function.
func (m *fsm) invoke() (state, error) {
	fmt.Println("ENTER STATE: invoke")

	ctx, cancel := context.WithCancel(m.ctx)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- m.fn(ctx)
	}()

	for {
		select {
		case <-m.ctx.Done():
			return nil, m.ctx.Err()

		case tok, ok := <-m.tokens:
			if ok {
				fmt.Println("IGNORED EXTRA TOKEN")

				// Any additional tokens we receive must be duplicates as we're
				// already holding the token that granted us the lock in m.token.
				if err := tok.Ack(false); err != nil { // false = single message
					return nil, err
				}
			} else {
				// If m.tokens is closed before m.closed becomes readable
				// we don't want to attempt to read from it anymore, just wait
				// for the error to appear or the context to be canceled.
				m.tokens = nil
			}

		case err := <-result:
			return nil, err

		case err := <-m.done:
			return nil, err
		}
	}
}

func (m *fsm) declare() error {
	_, err := m.tc.QueueDeclare(
		m.res+".lock",
		false, // durable,
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	q, err := m.tc.QueueDeclare(
		m.res+".tokens",
		false, // durable,
		false, // auto-delete
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
		fmt.Println("PUBLISH TOKEN")

		err = m.tc.Publish(
			"",
			m.res+".tokens",
			false, // mandatory
			false, // immediate
			amqp.Publishing{},
		)
		if err != nil {
			return err
		}
	}

	m.tokens, err = m.tc.Consume(
		m.res+".tokens",
		m.res,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait bool
		nil,   // args
	)
	if err != nil {
		return err
	}

	return err
}
