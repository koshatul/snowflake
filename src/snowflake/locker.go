package snowflake

import "context"

// Locker is used to acquire locks on named resources.
type Locker interface {
	// Do calls fn while holding a lock on the resource named r.
	//
	// It blocks until the lock is acquired and fn returns, or until ctx is
	// cancelled.
	//
	// c, the context passed to fn, is derived from ctx and is canceled when
	// the lock is unexpedly released before fn returns, such as when
	// communications with the lock coordinator are severed.
	Do(ctx context.Context, r string, fn func(c context.Context)) error
}
