package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/SonabaTeam/dqueue"
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
	db  *sql.DB
	log *slog.Logger
}

func NewMySQL(credentials MySQLCredentials, log *slog.Logger) (*DB, error) {
	return newDatabase("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", credentials.Username, credentials.Password, credentials.Host, fmt.Sprint(credentials.Port), credentials.Database), log)
}

func NewSQLite(credentials SQLiteCredentials, log *slog.Logger) (*DB, error) {
	return newDatabase("sqlite", fmt.Sprintf("file:%s", credentials.File), log)
}

func newDatabase(driver string, dsn string, log *slog.Logger) (*DB, error) {
	log.Debug("Connecting database...")
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	if err2 := db.Ping(); err2 != nil {
		return nil, err2
	}
	log.Info("Successfully connected database")
	if driver == "mysql" {
		var version string
		err3 := db.QueryRow("SELECT VERSION()").Scan(&version)
		if err3 != nil {
			return nil, err3
		}
		log.Info("MySQL Version: " + version)
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
	return &DB{db: db, log: log}, nil
}

func (d *DB) Close() error {
	d.log.Debug("Closing database...")
	err := d.db.Close()
	if err != nil {
		d.log.Error("Failed close database: ", err.Error())
	} else {
		d.log.Info("Successfully disconnected database")
	}
	return err
}

func (d *DB) GetDB() *sql.DB {
	return d.db
}

func (d *DB) ExecSelect(callback func([]map[string]any, error), query string, args ...any) {
	if callback != nil {
		res, err := d.execSelect(query, args...)
		callback(res, err)
	}
}

func (d *DB) execSelect(query string, args ...any) ([]map[string]any, error) {
	var res []map[string]any
	var errr error
	dqueue.Push(func() {
		rows, err := d.db.Query(query, args...)
		if err != nil {
			res = nil
			errr = err
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
					if b, ok := values[i].([]byte); ok {
						row[col] = string(b)
					} else {
						row[col] = values[i]
					}
				}
				result = append(result, row)
			}
			res = result
			errr = nil
		}
	}, 0*time.Second, false)
	return res, errr
}

func (d *DB) Exec(callback func(sql.Result, error), query string, args ...any) {
	res, err := d.exec(query, args...)
	if callback != nil {
		callback(res, err)
	}
}

func (d *DB) exec(query string, args ...any) (sql.Result, error) {
	done := make(chan struct{})
	var res sql.Result
	var err error

	dqueue.Push(func() {
		res, err = d.db.Exec(query, args...)
		close(done)
	}, 0*time.Second, false)

	<-done
	return res, err
}
