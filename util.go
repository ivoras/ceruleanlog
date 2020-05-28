package main

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"log"
	"math/rand"
	"os"
	"time"
)

// Seeds the math.Rand() generator.
func InitRandom() {
	hn, err := os.Hostname()
	s := time.Now().UTC().UnixNano()
	if err == nil {
		s += int64(crc32.ChecksumIEEE([]byte(hn)))
	}
	rand.Seed(s)
}

// AbsInt64 returns an absolute value of an int64
func AbsInt64(n int64) int64 {
	y := n >> 63       // y ← x ⟫ 63
	return (n ^ y) - y // (x ⨁ y) - y
}

// AbsInt32 returns an sbsolute value of an int32
func AbsInt32(n int32) int32 {
	y := n >> 31       // y ← x ⟫ 31
	return (n ^ y) - y // (x ⨁ y) - y
}

// BToMB divides the given number by (1024*1024)
func BToMB(b uint64) uint64 {
	return b / (1024 * 1024)
}

func jsonifyWhatever(i interface{}) string {
	jsonb, err := json.Marshal(i)
	if err != nil {
		log.Panic(err)
	}
	return string(jsonb)
}

func jsonifyWhateverToBytes(i interface{}) []byte {
	jsonb, err := json.Marshal(i)
	if err != nil {
		log.Panic(err)
	}
	return jsonb
}

// jsonifyWhateverToBuffer converts whatever is passed into a
// JSON byte buffer.
func jsonifyWhateverToBuffer(i interface{}) *bytes.Buffer {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(i)
	return b
}

func toJSONBuffer(i interface{}) *bytes.Buffer {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(i)
	return b
}
