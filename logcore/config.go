package logcore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/snabb/isoweek"
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

type spanNameID struct {
	name string
	id   uint32
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

func (c CeruleanConfig) ShardNameToTsID(name string) (ts, id uint32, err error) {
	var t time.Time
	switch c.ShardTimeSpec {
	case ShardTimeSpecYear:
		t, err = time.ParseInLocation("2006", name, time.UTC)
		ts = uint32(t.Unix())
		id = uint32(t.Year())
	case ShardTimeSpecMonth:
		t, err = time.ParseInLocation("2006-01", name, time.UTC)
		ts = uint32(t.Unix())
		id = uint32(uint32(t.Year())*100 + uint32(t.Month()))
	case ShardTimeSpecWeek:
		var y, w int
		if _, err = fmt.Sscanf(name, "%4d-W%2d", &y, &w); err != nil {
			err = fmt.Errorf("Cannot scan week spec %s: %w", name, err)
			return
		}
		t = isoweek.StartTime(y, w, time.UTC)
		ts = uint32(t.Unix())
		id = uint32(y)*100 + uint32(w)
	case ShardTimeSpecDay:
		t, err = time.ParseInLocation("2006-01-02", name, time.UTC)
		ts = uint32(t.Unix())
		id = ts / (3600 * 24)
	default:
		log.Panicln("Invalid ShardTimeSpec:", c.ShardTimeSpec)
	}
	return
}

// GetShardName returns a name and a unique ID
// (the name and the ID are locally unique and date-based)
// for a shard which contains data for the given timestamp.
func (c CeruleanConfig) GetShardNameID(ts uint32) (name string, id uint32) {
	t := unixTimeStampToUTCTime(ts)
	switch c.ShardTimeSpec {
	case ShardTimeSpecYear:
		return t.Format("2006"), uint32(t.Year())
	case ShardTimeSpecMonth:
		return t.Format("2006-01"), uint32(t.Year())*100 + uint32(t.Month())
	case ShardTimeSpecWeek:
		y, w := t.ISOWeek()
		return fmt.Sprintf("%04d-W%02d", y, w), uint32(y)*100 + uint32(w)
	case ShardTimeSpecDay:
		return t.Format("2006-01-02"), ts / (3600 * 24)
	default:
		log.Panicln("Invalid ShardTimeSpec:", c.ShardTimeSpec)
	}
	return
}

func (c CeruleanConfig) GetShardNameIDsTimeSpan(timeFrom, timeTo uint32) (list []spanNameID) {
	list = []spanNameID{}
	skip := uint32(0)
	switch c.ShardTimeSpec {
	case ShardTimeSpecYear:
		skip = 3600 * 24 * 365
	case ShardTimeSpecMonth:
		skip = 3600 * 24 * 28
	case ShardTimeSpecWeek:
		skip = 3600 * 24 * 6
	case ShardTimeSpecDay:
		skip = 3600 * 23
	default:
		log.Panicln("Invalid ShardTimeSpec:", c.ShardTimeSpec)
	}
	oldID := uint32(0)
	for t := timeFrom; t < timeTo; t += skip {
		name, id := c.GetShardNameID(t)
		if id != oldID {
			list = append(list, spanNameID{name: name, id: id})
			oldID = id
		}
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
