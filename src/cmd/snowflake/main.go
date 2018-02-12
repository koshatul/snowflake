package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jmalloc/snowflake/src/snowflakeamqp"
)

func main() {
	locker := &snowflakeamqp.Locker{}

	ctx := context.Background()

	go locker.Run(ctx)

	for {
		err := locker.Do(ctx, "foo", func(ctx context.Context) {
			fmt.Println("WORK")

			select {
			case <-time.After(10 * time.Second):
				return
			case <-ctx.Done():
				return
			}
		})

		if err != nil {
			panic(err)
		}
	}
}
