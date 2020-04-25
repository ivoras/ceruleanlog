package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Shards are always time-based.

type DbShard struct {
	db            *sql.DB
	dataFields    []string // Must be kept sorted for binary search
	indexedFields []string // Must be kept sorted for binary search
}

type DbShardCollection struct {
	WithRWMutex

	shards map[string]DbShard
}

var shardCollection = DbShardCollection{shards: map[string]DbShard{}}

// GetShardDirName returns the directory name for a shard which contains data
// for the given timestamp.
func (sc *DbShardCollection) GetShardDirName(ts uint32) (dirName string) {
	return fmt.Sprintf("%s/%s", getShardsDir(), globalConfig.GetShardName(ts))
}

func (sc *DbShardCollection) GetShard(ts uint32) (shard DbShard, err error) {
	shardDir := sc.GetShardDirName(ts)
	if _, err = os.Stat(shardDir); err != nil {
		err = os.MkdirAll(shardDir, 0755)
		if err != nil {
			return
		}
	}
	shardDbName := fmt.Sprintf("%s/shard.db", shardDir)
	shardDbExists := false
	if _, err := os.Stat(shardDbName); err == nil {
		shardDbExists = true
	}
	err = nil

	db, err := sql.Open("sqlite3", shardDbName)
	if err != nil {
		return
	}
	if !shardDbExists {
		_, err = db.Exec(fmt.Sprintf("PRAGMA journal_mode=%s", globalConfig.SQLiteJournalMode))
		if err != nil {
			return
		}
		_, err = db.Exec(`
		CREATE TABLE data(
			id 				INTEGER PRIMARY KEY,
			timestamp 		INTEGER NOT NULL,
			host 			TEXT,
			full_message 	TEXT,
			short_message	TEXT
		);
		CREATE INDEX ON data(timestamp);
		CREATE INDEX ON data(host);
		`)
		if err != nil {
			return
		}
		shard.dataFields = []string{"full_message", "host", "short_message", "timestamp"}
		shard.indexedFields = []string{"host", "timestamp"}
	} else {
		// Load dataFields and indexedFields from database
		shard.dataFields = []string{}
		var rows *sql.Rows
		rows, err = db.Query("PRAGMA table_info(data)")
		if err != nil {
			return
		}
		for rows.Next() {
			var col struct {
				idx      int
				name     string
				type_    string
				notnull  int
				default_ string
				ispk     int
			}
			err = rows.Scan(&col.idx, &col.name, &col.type_, &col.notnull, &col.default_, &col.ispk)
			if err != nil {
				return
			}
			shard.dataFields = InsertSortedString(col.name, shard.dataFields)
		}
		shard.indexedFields = []string{}
		rows, err = db.Query("PRAGMA index_list(data)")
		if err != nil {
			return
		}
		indexList := []string{}
		for rows.Next() {
			var idx struct {
				seq    int
				name   string
				unique int
			}
			err = rows.Scan(&idx.seq, &idx.name, &idx.unique)
			if err != nil {
				return
			}
			indexList = append(indexList, idx.name)
		}
		for _, idxName := range indexList {
			rows, err = db.Query("PRAGMA index_info(" + idxName + ")")
			if err != nil {
				return
			}
			for rows.Next() {
				var col struct {
					seq  int
					cid  int
					name string
				}
				err = rows.Scan(&col.seq, &col.cid, &col.name)
				if err != nil {
					return
				}
				shard.indexedFields = InsertSortedString(col.name, shard.indexedFields)
			}
		}
	}
	shard.db = db
	return
}

func CommitMessageToShards(msg BasicGelfMessage) (err error) {
	if msg.Timestamp == 0 {
		msg.Timestamp = uint32(getNowUTC())
	}
	shard, err := shardCollection.GetShard(msg.Timestamp)
	if err != nil {
		return
	}
	/*
		fields := make([]string, len(shard.dataFields))
		for i, fn := range shard.dataFields {
			fields[i] = fn
		}*/
	fields := shard.dataFields
	// Step 1: find out if the message has additional fields which are not present in the database
	newFields := map[string]string{}
	for fn := range msg.AdditionalNumbers {
		if !InStringArraySorted(fn, fields) {
			newFields[fn] = "NUMERIC"
		}
	}
	for fn := range msg.AdditionalStrings {
		if !InStringArraySorted(fn, fields) {
			newFields[fn] = "TEXT"
		}
	}
	for fn, fnType := range newFields {
		fields = InsertSortedString(fn, fields)
		_, err = shard.db.Exec(fmt.Sprintf("ALTER TABLE data ADD COLUMN %s %s", fn, fnType))
		if err != nil {
			return
		}
	}
	shard.dataFields = fields
	values := make([]string, len(fields))
	for i, fn := range fields {
		switch fn {
		case "full_message":
			values[i] = quoteSQLString(msg.FullMessage)
		case "host":
			values[i] = quoteSQLString(msg.Host)
		case "short_message":
			values[i] = quoteSQLString(msg.ShortMessage)
		case "timestamp":
			values[i] = strconv.Itoa(int(msg.Timestamp))
		default:
			if v, found := msg.AdditionalNumbers[fn]; found {
				values[i] = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				values[i] = quoteSQLString(msg.AdditionalStrings[fn])
			}
		}
	}
	sqlString := fmt.Sprintf("INSERT INTO data(%s) VALUES(%s)", strings.Join(fields, ","), strings.Join(values, ","))
	_, err = shard.db.Exec(sqlString)

	return
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "''"
}
