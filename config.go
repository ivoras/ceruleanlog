package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

type ShardTimeSpecType uint32

const (
	ShardTimeSpecYear ShardTimeSpecType = iota
	ShardTimeSpecMonth
	ShardTimeSpecWeek
	ShardTimeSpecDay
)

type CeruleanConfig struct {
	SQLiteJournalMode       string            `json:"sqlite_journal_mode"`
	ShardTimeSpecString     string            `json:"shard_time_spec"`
	ShardTimeSpec           ShardTimeSpecType `json:"-"`
	MemoryBufferTimeSeconds uint32            `json:"memory_buffer_time_seconds"`
	IndexFieldList          []string          `json:"index_field_list"`
}

func ReadCeruleanConfig(fileName string) (cfg CeruleanConfig, err error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return
	}
	switch cfg.ShardTimeSpecString {
	case "year":
		cfg.ShardTimeSpec = ShardTimeSpecYear
	case "month":
		cfg.ShardTimeSpec = ShardTimeSpecMonth
	case "week":
		cfg.ShardTimeSpec = ShardTimeSpecWeek
	case "day":
		cfg.ShardTimeSpec = ShardTimeSpecDay
	default:
		err = fmt.Errorf("Invalid shard_time_spec: %s", cfg.ShardTimeSpec)
		return
	}
	if !InStringArray(cfg.SQLiteJournalMode, []string{"wal", "delete", "memory"}) {
		err = fmt.Errorf("Invalid or unsupported sqlite_journal_mode: %s", cfg.SQLiteJournalMode)
		return
	}
	return
}

func WriteCeruleanConfig(fileName string, cfg CeruleanConfig) (err error) {
	data, err := json.Marshal(&cfg)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(fileName, data, 0644)
	return
}

func NewCeruleanConfig() (cfg CeruleanConfig) {
	cfg.SQLiteJournalMode = "delete"
	cfg.ShardTimeSpecString = "week"
	cfg.ShardTimeSpec = ShardTimeSpecWeek
	cfg.MemoryBufferTimeSeconds = 30
	cfg.IndexFieldList = []string{}
	return
}

func getConfigFileName() string {
	return fmt.Sprintf("%s/%s", *dataDir, *configFile)
}

// GetShardName returns a name for a shard (the name is unique and date-based)
// which contains data for the given timestamp.
func (c CeruleanConfig) GetShardName(ts uint32) (name string) {
	t := unixTimeStampToUTCTime(ts)
	switch c.ShardTimeSpec {
	case ShardTimeSpecYear:
		return t.Format("2006")
	case ShardTimeSpecMonth:
		return t.Format("2006-01")
	case ShardTimeSpecWeek:
		y, w := t.ISOWeek()
		return fmt.Sprintf("%04d-%02d", y, w)
	case ShardTimeSpecDay:
		return t.Format("2006-01-02")
	default:
		log.Panicln("Invalid ShardTimeSpec:", c.ShardTimeSpec)
	}
	return
}
