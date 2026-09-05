package db

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
)

// newTestLogger returns a logger that discards output so test runs stay quiet.
// Swap io.Discard for os.Stdout locally if you need to debug a failing test.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// setupUsersTable creates a minimal users table, tolerant of the small
// syntax differences between SQLite and MySQL auto-increment columns.
func setupUsersTable(t *testing.T, d *DB) {
	t.Helper()

	_, err := d.ExecQuery(context.Background(), `
		CREATE TABLE IF NOT EXISTS users (
			id   INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			age  INTEGER NOT NULL
		)`)
	if err != nil {
		// MySQL doesn't understand AUTOINCREMENT (it's AUTO_INCREMENT),
		// so fall back to the MySQL-friendly form if the first attempt failed.
		_, err = d.ExecQuery(context.Background(), `
			CREATE TABLE IF NOT EXISTS users (
				id   INTEGER PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(255) NOT NULL,
				age  INTEGER NOT NULL
			)`)
	}
	if err != nil {
		t.Fatalf("failed to create users table: %v", err)
	}
}

// runCRUDSuite exercises insert/select/update/delete and the async
// variants against any *DB, regardless of the underlying driver.
func runCRUDSuite(t *testing.T, d *DB) {
	t.Helper()
	setupUsersTable(t, d)

	t.Run("insert and select", func(t *testing.T) {
		res, err := d.ExecQuery(context.Background(),
			"INSERT INTO users (name, age) VALUES (?, ?)", "alice", 30)
		if err != nil {
			t.Fatalf("insert error = %v", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatalf("LastInsertId() error = %v", err)
		}
		if id == 0 {
			t.Error("expected non-zero insert id")
		}

		rows, err := d.QuerySelect(context.Background(),
			"SELECT id, name, age FROM users WHERE name = ?", "alice")
		if err != nil {
			t.Fatalf("select error = %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["name"] != "alice" {
			t.Errorf("expected name = alice, got %v", rows[0]["name"])
		}
	})

	t.Run("update", func(t *testing.T) {
		_, err := d.ExecQuery(context.Background(),
			"INSERT INTO users (name, age) VALUES (?, ?)", "bob", 25)
		if err != nil {
			t.Fatalf("insert error = %v", err)
		}

		res, err := d.ExecQuery(context.Background(),
			"UPDATE users SET age = ? WHERE name = ?", 26, "bob")
		if err != nil {
			t.Fatalf("update error = %v", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() error = %v", err)
		}
		if affected != 1 {
			t.Errorf("expected 1 row affected, got %d", affected)
		}

		rows, err := d.QuerySelect(context.Background(),
			"SELECT age FROM users WHERE name = ?", "bob")
		if err != nil {
			t.Fatalf("select error = %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		age, ok := rows[0]["age"].(int64)
		if !ok || age != 26 {
			t.Errorf("expected age = 26, got %v (%T)", rows[0]["age"], rows[0]["age"])
		}
	})

	t.Run("delete", func(t *testing.T) {
		_, err := d.ExecQuery(context.Background(),
			"INSERT INTO users (name, age) VALUES (?, ?)", "carol", 40)
		if err != nil {
			t.Fatalf("insert error = %v", err)
		}

		res, err := d.ExecQuery(context.Background(),
			"DELETE FROM users WHERE name = ?", "carol")
		if err != nil {
			t.Fatalf("delete error = %v", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("RowsAffected() error = %v", err)
		}
		if affected != 1 {
			t.Errorf("expected 1 row affected, got %d", affected)
		}

		rows, err := d.QuerySelect(context.Background(),
			"SELECT * FROM users WHERE name = ?", "carol")
		if err != nil {
			t.Fatalf("select error = %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("expected 0 rows after delete, got %d", len(rows))
		}
	})

	t.Run("select no rows returns empty slice not nil error", func(t *testing.T) {
		rows, err := d.QuerySelect(context.Background(),
			"SELECT * FROM users WHERE name = ?", "nobody")
		if err != nil {
			t.Fatalf("select error = %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("expected 0 rows, got %d", len(rows))
		}
	})

	t.Run("invalid query returns error", func(t *testing.T) {
		_, err := d.QuerySelect(context.Background(), "SELECT * FROM no_such_table")
		if err == nil {
			t.Fatal("expected error for query against nonexistent table, got nil")
		}
	})

	t.Run("ExecSelect callback style", func(t *testing.T) {
		_, err := d.ExecQuery(context.Background(),
			"INSERT INTO users (name, age) VALUES (?, ?)", "dave", 50)
		if err != nil {
			t.Fatalf("insert error = %v", err)
		}

		done := make(chan struct{})
		var gotRows []map[string]any
		var gotErr error
		d.ExecSelect(func(rows []map[string]any, err error) {
			gotRows, gotErr = rows, err
			close(done)
		}, "SELECT name FROM users WHERE name = ?", "dave")
		<-done

		if gotErr != nil {
			t.Fatalf("ExecSelect callback error = %v", gotErr)
		}
		if len(gotRows) != 1 || gotRows[0]["name"] != "dave" {
			t.Errorf("unexpected result: %v", gotRows)
		}
	})

	t.Run("ExecSelectAsync does not block caller", func(t *testing.T) {
		done := make(chan error, 1)
		d.ExecSelectAsync(func(rows []map[string]any, err error) {
			done <- err
		}, "SELECT 1")

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("ExecSelectAsync callback error = %v", err)
			}
		case <-context.Background().Done():
			t.Fatal("ExecSelectAsync callback never fired")
		}
	})

	t.Run("ExecAsync does not block caller", func(t *testing.T) {
		done := make(chan error, 1)
		d.ExecAsync(func(res sql.Result, err error) {
			done <- err
		}, "INSERT INTO users (name, age) VALUES (?, ?)", "erin", 22)

		if err := <-done; err != nil {
			t.Errorf("ExecAsync callback error = %v", err)
		}
	})
}
