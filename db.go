package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

// DB wraps a *sql.DB connection pool along with a logger and an optional
// background heartbeat goroutine used to detect dead connections early.
type DB struct {
	db      *sql.DB
	log     *slog.Logger
	opts    Options
	cancel  context.CancelFunc // used to stop the heartbeat goroutine
	wg      sync.WaitGroup
	writeMu sync.Mutex // serializes writes; only used for SQLite
	driver  string
}

// NewMySQL opens a connection pool to a MySQL database using the given
// credentials and returns a ready-to-use *DB. It verifies connectivity
// with a Ping and logs the server version before returning.
func NewMySQL(credentials MySQLCredentials, log *slog.Logger, opts ...Options) (*DB, error) {
	o := DefaultOptions()
	if len(opts) > 0 {
		o = mergeOptions(o, opts[0])
	}

	// Use mysql.Config instead of hand-crafting the DSN string:
	// avoids breaking the DSN when the password contains special characters
	// like @, :, or /, and lets us set parseTime and driver-level timeouts.
	cfg := mysql.NewConfig()
	cfg.User = credentials.Username
	cfg.Passwd = credentials.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", credentials.Host, credentials.Port)
	cfg.DBName = credentials.Database
	cfg.ParseTime = true
	cfg.Timeout = o.DialTimeout
	cfg.ReadTimeout = o.ReadTimeout
	cfg.WriteTimeout = o.WriteTimeout

	return newDatabase("mysql", cfg.FormatDSN(), log, o)
}

// NewSQLite opens a connection to a SQLite database file and returns a
// ready-to-use *DB. A busy_timeout pragma is set to avoid immediate
// "database is locked" errors under light concurrent access.
func NewSQLite(credentials SQLiteCredentials, log *slog.Logger, opts ...Options) (*DB, error) {
	o := DefaultOptions()
	if len(opts) > 0 {
		o = mergeOptions(o, opts[0])
	}
	return newDatabase("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)", credentials.File), log, o)
}

// newDatabase is the shared setup path for both drivers: it opens the
// connection, verifies it with a Ping, applies pool settings, and starts
// the optional heartbeat goroutine. opts is expected to already be a fully
// merged Options (see NewMySQL/NewSQLite), so every pointer field here is
// guaranteed non-nil.
func newDatabase(driver string, dsn string, log *slog.Logger, o Options) (*DB, error) {
	log.Debug("Connecting database...")
	sqlDB, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.DialTimeout)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	log.Info("Successfully connected database")

	if driver == "mysql" {
		var version string
		if err := sqlDB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
			_ = sqlDB.Close()
			return nil, err
		}
		log.Info("MySQL Version: " + version)
	}

	// SQLite uses a single-file locking model, so a large connection pool
	// only increases lock contention instead of helping throughput.
	if driver == "sqlite" {
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	} else {
		sqlDB.SetMaxIdleConns(o.MaxIdleConns)
		sqlDB.SetMaxOpenConns(o.MaxOpenConns)
	}
	sqlDB.SetConnMaxLifetime(o.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(o.ConnMaxIdleTime)

	d := &DB{db: sqlDB, log: log, opts: o, driver: driver}

	if o.PingInterval > 0 {
		heartbeatCtx, cancel := context.WithCancel(context.Background())
		d.cancel = cancel
		d.wg.Add(1)
		go d.heartbeat(heartbeatCtx)
	}

	return d, nil
}

// heartbeat does low-frequency liveness checks, mainly to catch a dead
// connection early (e.g. database restart, or a proxy silently dropping
// a long-idle connection). It's not meant to detect failures within
// milliseconds, and a failed ping here does not necessarily mean the
// database is down — see the skip logic below. It exits promptly on
// ctx.Done() so it never leaks after Close().
func (d *DB) heartbeat(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.opts.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Skip this tick if the pool is saturated. Pinging under
			// contention would just compete with real traffic for a
			// connection, and a timeout here would reflect pool pressure,
			// not an actual database failure — reporting it as a failure
			// would just be a false alarm.
			stats := d.db.Stats()
			if stats.MaxOpenConnections > 0 && stats.InUse >= stats.MaxOpenConnections {
				continue
			}
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := d.db.PingContext(pingCtx); err != nil {
				d.log.Warn("Database ping failed", slog.String("error", err.Error()))
			}
			cancel()
		}
	}
}

// Close stops the heartbeat goroutine (if running) and closes the
// underlying connection pool. It waits for the heartbeat goroutine to
// fully exit before returning, so no goroutine is ever left dangling.
func (d *DB) Close() error {
	d.log.Debug("Closing database...")
	if d.cancel != nil {
		d.cancel()
		d.wg.Wait() // make sure the heartbeat goroutine has actually exited, no dangling goroutine
	}
	err := d.db.Close()
	if err != nil {
		d.log.Error("Failed to close database", slog.String("error", err.Error()))
	} else {
		d.log.Info("Successfully disconnected database")
	}
	return err
}

// DB returns the underlying *sql.DB, for callers who need direct access
// to standard library features not exposed by this wrapper (e.g. Stats(),
// BeginTx, or driver-specific behavior).
func (d *DB) DB() *sql.DB {
	return d.db
}

// ExecSelectAsync runs a SELECT query in a separate goroutine and delivers
// the result via callback without blocking the caller. Use this when the
// caller doesn't need to wait for the query to complete.
func (d *DB) ExecSelectAsync(callback func(res []map[string]any, err error), query string, args ...any) {
	go func() {
		d.ExecSelect(callback, query, args...)
	}()
}

// ExecSelect runs a SELECT query synchronously and passes the result to
// callback. Each row is returned as a map keyed by column name.
func (d *DB) ExecSelect(callback func(res []map[string]any, err error), query string, args ...any) {
	res, err := d.QuerySelect(context.Background(), query, args...)
	if callback != nil {
		callback(res, err)
	}
}

// QuerySelect runs a SELECT query with the given context and returns each
// row as a map keyed by column name. []byte values are converted to string
// for convenience. The query is subject to d.opts.QueryTimeout if it is
// greater than zero.
func (d *DB) QuerySelect(ctx context.Context, query string, args ...any) (res []map[string]any, err error) {
	if timeout := d.opts.QueryTimeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, 16) // pre-allocate to reduce append reallocations
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(cols))
		for i, col := range cols {
			if b, ok := values[i].([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = values[i]
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// ExecAsync runs a write query (INSERT/UPDATE/DELETE/DDL) in a separate
// goroutine and delivers the result via callback without blocking the caller.
func (d *DB) ExecAsync(callback func(res sql.Result, err error), query string, args ...any) {
	go func() {
		d.Exec(callback, query, args...)
	}()
}

// Exec runs a write query (INSERT/UPDATE/DELETE/DDL) synchronously and
// passes the result to callback.
func (d *DB) Exec(callback func(res sql.Result, err error), query string, args ...any) {
	res, err := d.ExecQuery(context.Background(), query, args...)
	if callback != nil {
		callback(res, err)
	}
}

// ExecQuery runs a write query (INSERT/UPDATE/DELETE/DDL) with the given
// context and returns the sql.Result. The query is subject to
// d.opts.QueryTimeout if it is greater than zero. Writes against SQLite are
// serialized through writeMu since SQLite only supports a single writer.
func (d *DB) ExecQuery(ctx context.Context, query string, args ...any) (res sql.Result, err error) {
	if timeout := d.opts.QueryTimeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if d.driver == "sqlite" {
		d.writeMu.Lock()
		defer d.writeMu.Unlock()
	}
	return d.db.ExecContext(ctx, query, args...)
}
