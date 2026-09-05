package db

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func newSQLiteTestDB(t *testing.T) *DB {
	t.Helper()

	file := filepath.Join(t.TempDir(), "test.db")

	d, err := NewSQLite(SQLiteCredentials{File: file}, newTestLogger(), Options{
		PingInterval: 0, // disable heartbeat in tests, we don't need it and it would outlive the test
		QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return d
}

func TestSQLite_CRUD(t *testing.T) {
	d := newSQLiteTestDB(t)
	runCRUDSuite(t, d)
}

func TestSQLite_ConcurrentInserts(t *testing.T) {
	d := newSQLiteTestDB(t)

	_, err := d.ExecQuery(context.Background(), `
		CREATE TABLE IF NOT EXISTS users (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			age  INTEGER NOT NULL
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	const n = 20
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			_, err := d.ExecQuery(context.Background(),
				"INSERT INTO users (name, age) VALUES (?, ?)",
				fmt.Sprintf("user-%d", i), i) // 唯一 name，防止无法区分重复
			done <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Errorf("concurrent insert %d failed: %v", i, err)
		}
	}

	rows, err := d.QuerySelect(context.Background(), "SELECT COUNT(*) AS c FROM users")
	if err != nil {
		t.Fatalf("QuerySelect() error = %v", err)
	}
	if got := rows[0]["c"]; got != int64(n) {
		t.Errorf("expected %d rows, got %v (%T)", n, got, got)
	}
}

func TestSQLite_CloseStopsHeartbeat(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "heartbeat.db")

	d, err := NewSQLite(SQLiteCredentials{File: file}, newTestLogger(), Options{
		PingInterval: 10 * time.Millisecond, // heartbeat enabled this time
		QueryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSQLite() error = %v", err)
	}

	// Let the heartbeat tick a few times before closing.
	time.Sleep(50 * time.Millisecond)

	if err := d.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Close() waits on d.wg internally, so if the heartbeat goroutine were
	// still running this test would hang instead of failing outright.
	// Reaching this point means the goroutine exited cleanly.
}
