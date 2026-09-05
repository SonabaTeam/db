package db

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// mysqlTestCredentials builds MySQLCredentials from environment variables
// and skips the test if MYSQL_TEST_HOST is not set, so these tests never
// fail in environments without a MySQL instance available.
//
// Example local run with Docker:
//
//	docker run --rm -e MYSQL_ROOT_PASSWORD=test -e MYSQL_DATABASE=testdb \
//	  -p 3306:3306 mysql:8
//	MYSQL_TEST_HOST=127.0.0.1 MYSQL_TEST_PORT=3306 MYSQL_TEST_USER=root \
//	  MYSQL_TEST_PASSWORD=test MYSQL_TEST_DATABASE=testdb go test ./...
func mysqlTestCredentials(t *testing.T) MySQLCredentials {
	t.Helper()

	host := os.Getenv("MYSQL_TEST_HOST")
	if host == "" {
		t.Skip("MYSQL_TEST_HOST not set, skipping MySQL integration test")
	}

	port := 3306
	if p := os.Getenv("MYSQL_TEST_PORT"); p != "" {
		parsed, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("invalid MYSQL_TEST_PORT %q: %v", p, err)
		}
		port = parsed
	}

	user := os.Getenv("MYSQL_TEST_USER")
	if user == "" {
		user = "root"
	}
	database := os.Getenv("MYSQL_TEST_DATABASE")
	if database == "" {
		database = "testdb"
	}

	return MySQLCredentials{
		Host:     host,
		Port:     port,
		Username: user,
		Password: os.Getenv("MYSQL_TEST_PASSWORD"),
		Database: database,
	}
}

func newMySQLTestDB(t *testing.T) *DB {
	t.Helper()

	creds := mysqlTestCredentials(t)
	d, err := NewMySQL(creds, newTestLogger(), Options{
		PingInterval: 0, // disable heartbeat in tests
		QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewMySQL() error = %v", err)
	}
	t.Cleanup(func() {
		// Best-effort cleanup so repeated runs against the same database
		// don't accumulate leftover rows/tables.
		_, _ = d.ExecQuery(context.Background(), "DROP TABLE IF EXISTS users")
		if err := d.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return d
}

func TestMySQL_CRUD(t *testing.T) {
	d := newMySQLTestDB(t)
	runCRUDSuite(t, d)
}

func TestMySQL_QueryTimeout(t *testing.T) {
	d := newMySQLTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// SLEEP(1) forces the query to run longer than the context deadline.
	_, err := d.QuerySelect(ctx, "SELECT SLEEP(1)")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestMySQL_ConnectionPoolLimits(t *testing.T) {
	d := newMySQLTestDB(t)
	stats := d.DB().Stats()
	want := DefaultOptions().MaxOpenConns
	if stats.MaxOpenConnections != want {
		t.Errorf("expected MaxOpenConnections = %d, got %d", want, stats.MaxOpenConnections)
	}
}
