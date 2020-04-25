package main

import (
	"database/sql"
)

type DbShard struct {
	db *sql.DB
	dataFields []string
}
