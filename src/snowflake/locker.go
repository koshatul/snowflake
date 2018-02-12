package snowflake

import (
	"context"
)

// Locker is used to acquire locks on named resources.
type Locker interface {
	// Do calls fn while holding a lock on the resource named r.
	//
	// It blocks until the lock is acquired or ctx is canceled. Once the lock
	// has been acquired fn is called with c, a context derived from c.
	//
	// If the lock is released while fn is executing, c is canceled. Once fn has
	// been called, Do() always blocks until fn returns.
	Do(ctx context.Context, r string, fn func(c context.Context)) error
}
