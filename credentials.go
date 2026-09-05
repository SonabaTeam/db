package db

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
