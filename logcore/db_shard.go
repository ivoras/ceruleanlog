package logcore

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// Shards are always time-based.

type DbShard struct {
	db            *sql.DB
	id            uint32
	name          string
	dataFields    []string // Must be kept sorted for binary search
	indexedFields []string // Must be kept sorted for binary search
}

type DbShardCollection struct {
	WithRWMutex

	shards   map[uint32]*DbShard
	instance *CeruleanInstance
}

func (sc *DbShardCollection) GetShard(ts uint32) (shard *DbShard, err error) {
	shardName, shardID := sc.instance.config.GetShardNameID(ts)
	var found bool
	sc.WithRLock(func() {
		shard, found = sc.shards[shardID]
	})
	if found {
		return
	}

	shardDir := fmt.Sprintf("%s/%s", sc.instance.getShardsDir(), shardName)
	if _, err = os.Stat(shardDir); err != nil {
		err = os.MkdirAll(shardDir, 0755)
		if err != nil {
			return
		}
	}
	shardDbFileName := fmt.Sprintf("%s/shard.db", shardDir)
	shardDbExists := false
	if _, err := os.Stat(shardDbFileName); err == nil {
		shardDbExists = true
	}
	err = nil

	db, err := sql.Open("sqlite3", shardDbFileName)
	if err != nil {
		return
	}
	shard = &DbShard{}
	if !shardDbExists {
		_, err = db.Exec(fmt.Sprintf("PRAGMA journal_mode=%s", sc.instance.config.SQLiteJournalMode))
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
		CREATE INDEX idx_data_timestamp ON data(timestamp);
		CREATE INDEX idx_data_host ON data(host);
		`)
		if err != nil {
			return
		}
		shard.dataFields = []string{"full_message", "host", "short_message", "timestamp"}
		shard.indexedFields = []string{"host", "timestamp"}
		log.Println("Created new shard database", shardDbFileName)
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
				default_ sql.NullString
				ispk     int
			}
			err = rows.Scan(&col.idx, &col.name, &col.type_, &col.notnull, &col.default_, &col.ispk)
			if err != nil {
				return
			}
			if col.name == "id" {
				continue
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
				seq     int
				name    string
				unique  int
				how     string
				partial int
			}
			err = rows.Scan(&idx.seq, &idx.name, &idx.unique, &idx.how, &idx.partial)
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
	shard.name = shardName
	shard.id = shardID
	sc.WithWLock(func() {
		sc.shards[shardID] = shard
	})
	return
}

func (sc *DbShardCollection) CommitMessagesToShards(messages *[]BasicGelfMessage) (err error) {
	var tx *sql.Tx
	oldShardID := uint32(0)

	for _, msg := range *messages {
		shard, err := sc.GetShard(msg.Timestamp)
		if err != nil {
			return err
		}
		if tx == nil {
			oldShardID = shard.id
			tx, err = shard.db.Begin()
			if err != nil {
				return err
			}
		} else if oldShardID != shard.id {
			err = tx.Commit()
			if err != nil {
				return err
			}
			tx, err = shard.db.Begin()
			if err != nil {
				return err
			}
			oldShardID = shard.id
		}

		sc.CommitMessageToShard(tx, &msg)
	}
	if tx != nil {
		err = tx.Commit()
	}
	return
}

func (sc *DbShardCollection) CommitMessageToShard(tx *sql.Tx, msg *BasicGelfMessage) (err error) {
	shard, err := sc.GetShard(msg.Timestamp)
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
		_, err = tx.Exec(fmt.Sprintf("ALTER TABLE data ADD COLUMN %s %s", fn, fnType))
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
		case "facility":
			values[i] = msg.Facility
		default:
			if v, found := msg.AdditionalNumbers[fn]; found {
				values[i] = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				// This accidentally works for fields which are not present in this message
				s := msg.AdditionalStrings[fn]
				if len(s) > 0 {
					values[i] = quoteSQLString(s)
				} else {
					values[i] = "NULL"
				}
			}
		}
	}
	sqlString := fmt.Sprintf("INSERT INTO data(%s) VALUES(%s)", strings.Join(fields, ","), strings.Join(values, ","))
	_, err = tx.Exec(sqlString)
	if err != nil {
		log.Println(sqlString)
	}

	return
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
