package db

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
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
	return &DB{db: db}, nil
}

func (d *DB) Select(query string, callback func([]map[string]any, error)) {
	go func() {
		rows, err := d.db.Query(query)
		if err != nil {
			callback(nil, err)
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
		}
	}()
}

func (d *DB) Exec(query string, callback func(sql.Result, error)) {
	go func() {
		res, err := d.db.Exec(query)
		callback(res, err)
	}()
}
