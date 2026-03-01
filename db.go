package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

type MySQLCredentials struct {
	Host     string
	Username string
	Password string
	Database string
	Port     int
}

type SQLiteCredentials struct {
	File string
}

type DB struct {
	db *sql.DB
}

func NewMySQL(credentials MySQLCredentials) (*DB, error) {
	return newDatabase("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", credentials.Username, credentials.Password, credentials.Host, fmt.Sprint(credentials.Port), credentials.Database))
}

func NewSQLite(credentials SQLiteCredentials) (*DB, error) {
	return newDatabase("sqlite", fmt.Sprintf("file:%s", credentials.File))
}

func newDatabase(driver string, dsn string) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err2 := db.Ping(); err2 != nil {
		return nil, err2
	}
	db.SetMaxIdleConns(20)
	db.SetMaxOpenConns(200)
	db.SetConnMaxLifetime(time.Hour)
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			db.Ping()
		}
	}()
	return &DB{db: db}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) GetDB() *sql.DB {
	return d.db
}

func (d *DB) ExecSelect(callback func([]map[string]any, error), query string, args ...any) {
	done := make(chan struct{})
	go func() {
		rows, err := d.db.Query(query, args...)
		if err != nil {
			callback(nil, err)
			close(done)
		} else {
			defer rows.Close()
			cols, _ := rows.Columns()
			result := []map[string]any{}
			for rows.Next() {
				values := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range values {
					ptrs[i] = &values[i]
				}
				rows.Scan(ptrs...)
				row := map[string]any{}
				for i, col := range cols {
					row[col] = values[i]
				}
				result = append(result, row)
			}
			callback(result, nil)
			close(done)
		}
	}()
	<-done
}

func (d *DB) Exec(callback func(sql.Result, error), query string, args ...any) {
	done := make(chan struct{})
	go func() {
		res, err := d.db.Exec(query, args...)
		callback(res, err)
		close(done)
	}()
	<-done
}
