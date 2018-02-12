package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jmalloc/snowflake/src/snowflakeamqp"
	"github.com/streadway/amqp"
)

func main() {
	broker, err := amqp.Dial("amqp://localhost")
	if err != nil {
		panic(err)
	}

	locker := &snowflakeamqp.Locker{Broker: broker}
	ctx := context.Background()

	for {
		err = locker.Do(ctx, "foo", func(ctx context.Context) error {
			fmt.Println("WORK")

			select {
			case <-time.After(10 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		if err != nil {
			panic(err)
		}
	}
}
