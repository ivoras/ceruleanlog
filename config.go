package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type CeruleanConfig struct {
	SQLiteJournalMode       string   `json:"sqlite_journal_mode"`
	ShardTimeSpec           string   `json:"shard_time_spec"`
	MemoryBufferTimeSeconds uint32   `json:"memory_buffer_time_seconds"`
	IndexFieldList          []string `json:"index_field_list"`
}

func ReadCeruleanConfig(fileName string) (cfg CeruleanConfig, err error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &cfg)
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
	cfg.ShardTimeSpec = "week"
	cfg.MemoryBufferTimeSeconds = 30
	cfg.IndexFieldList = []string{}
	return
}

func getConfigFileName() string {
	return fmt.Sprintf("%s/%s", *dataDir, *configFile)
}
