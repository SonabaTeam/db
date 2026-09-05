package db

import (
	"time"
)

// Options lets callers tune the connection pool, timeouts, and heartbeat
// behavior for their own workload, instead of having them hard-coded in
// the library.
//
// All fields are pointers so that a partial Options{} can be merged with
// DefaultOptions() without ambiguity: nil means "use the default", while a
// non-nil pointer — even one pointing at zero — means "the caller wants
// exactly this value", including disabling a feature entirely (e.g. an
// explicit zero ConnMaxLifetime means "never expire connections").
//
// Use the Dur() and Int() helpers below to build pointer values inline,
// e.g. Options{PingInterval: Dur(0)} to explicitly disable the heartbeat.
type Options struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	PingInterval    time.Duration
	QueryTimeout    time.Duration

	// DialTimeout, ReadTimeout, and WriteTimeout are MySQL-specific
	// driver-level timeouts. They only apply to NewMySQL — NewSQLite
	// ignores them. Unlike QueryTimeout (which is enforced via context and
	// therefore applies uniformly to any query), these are raw TCP-level
	// deadlines enforced by the driver itself, so they matter most for
	// long-running queries or large result sets that stream for a while.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DefaultOptions returns the library's recommended defaults for a
// long-lived, moderately loaded connection pool. Override individual
// fields as needed — see Options for how partial overrides are merged.
func DefaultOptions() Options {
	return Options{
		MaxIdleConns:    20,
		MaxOpenConns:    200,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 10 * time.Minute, // avoid connections idling long enough to get killed by firewalls/LBs
		PingInterval:    30 * time.Second, // low-frequency liveness check, not a keep-alive mechanism
		QueryTimeout:    10 * time.Second,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
	}
}

// mergeOptions returns a copy of base with every non-nil field from
// override applied on top. A nil field in override means "keep the base
// value"; a non-nil field — including one pointing at zero — always wins,
// so callers can explicitly disable a feature (e.g. ConnMaxLifetime: Dur(0)
// for "never expire") without it being silently replaced by the default.
func mergeOptions(base, override Options) Options {
	merged := base
	if override.MaxIdleConns > 0 {
		merged.MaxIdleConns = override.MaxIdleConns
	}
	if override.MaxOpenConns > 0 {
		merged.MaxOpenConns = override.MaxOpenConns
	}
	if override.ConnMaxLifetime > 0 {
		merged.ConnMaxLifetime = override.ConnMaxLifetime
	}
	if override.ConnMaxIdleTime > 0 {
		merged.ConnMaxIdleTime = override.ConnMaxIdleTime
	}
	if override.PingInterval > 0 {
		merged.PingInterval = override.PingInterval
	}
	if override.QueryTimeout > 0 {
		merged.QueryTimeout = override.QueryTimeout
	}
	if override.DialTimeout > 0 {
		merged.DialTimeout = override.DialTimeout
	}
	if override.ReadTimeout > 0 {
		merged.ReadTimeout = override.ReadTimeout
	}
	if override.WriteTimeout > 0 {
		merged.WriteTimeout = override.WriteTimeout
	}
	return merged
}
