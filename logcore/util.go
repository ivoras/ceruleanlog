package logcore

import (
	"bytes"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// WithMutex extends the Mutex type with the convenient .With(func) function
type WithMutex struct {
	sync.Mutex
}

// WithLock executes the given function with the mutex locked
func (m *WithMutex) WithLock(f func()) {
	m.Mutex.Lock()
	f()
	m.Mutex.Unlock()
}

// WithRWMutex extends the RWMutex type with convenient .With(func) functions
type WithRWMutex struct {
	sync.RWMutex
}

// WithRLock executes the given function with the mutex rlocked
func (m *WithRWMutex) WithRLock(f func()) {
	m.RWMutex.RLock()
	f()
	m.RWMutex.RUnlock()
}

// WithWLock executes the given function with the mutex wlocked
func (m *WithRWMutex) WithWLock(f func()) {
	m.RWMutex.Lock()
	f()
	m.RWMutex.Unlock()
}

type SortedStringSlice sort.StringSlice

func (ss *SortedStringSlice) Insert(s string) {
	idx := sort.Search(len(*ss), func(i int) bool {
		return (*ss)[i] >= s
	})
	*ss = append(*ss, "")
	copy((*ss)[idx+1:], (*ss)[idx:])
	(*ss)[idx] = s
	return
}

func (ss *SortedStringSlice) Sort() {
	(*sort.StringSlice)(ss).Sort()
}

// Converts the given Unix timestamp to time.Time
func unixTimeStampToUTCTime(ts uint32) time.Time {
	return time.Unix(int64(ts), 0)
}

// Gets the current Unix timestamp in UTC
func getNowUTC() int64 {
	return time.Now().UTC().Unix()
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

// HaversineDistance returns the distance in km between two coordinate pairs.
// See https://en.wikipedia.org/wiki/Haversine_formula .
func HaversineDistance(lat1, lng1, lat2, lng2 float64) float64 {
	R := 6371e3 // metres
	f1 := lat1 * math.Phi / 180
	f2 := lat2 * math.Phi / 180
	fdiff := (lat2 - lat1) * math.Pi / 180
	ddiff := (lng2 - lng1) * math.Pi / 180

	a := math.Sin(fdiff/2)*math.Sin(fdiff/2) +
		math.Cos(f1)*math.Cos(f2)*math.Sin(ddiff/2)*math.Sin(ddiff/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c / 1000 // in km
}

// IntArrayToString takes an []int and returns a string which consists of
// the integers converted to strings, delimited with delim, and prepended and
// appended with the specified open and end strings.
func IntArrayToString(x []int, delim string, open string, end string) string {
	return open + strings.Trim(strings.Join(strings.Fields(fmt.Sprint(x)), delim), "[]") + end
}

// IsIntArray checks to see if an int is in a []int.
func InIntArray(x int, a []int) bool {
	for _, n := range a {
		if n == x {
			return true
		}
	}
	return false
}

// InInt32Array checks to see if an int32 is in a []int32.
func InInt32Array(x int32, a []int32) bool {
	for _, n := range a {
		if n == x {
			return true
		}
	}
	return false
}

func InInt32ArraySorted(x int32, a []int32) bool {
	idx := sort.Search(len(a), func(i int) bool {
		return a[i] >= x
	})
	return idx < len(a) && a[idx] == x
}

// InInt64Array checks to see if an int64 is in a []int64.
func InInt64Array(x int64, a []int64) bool {
	for _, n := range a {
		if n == x {
			return true
		}
	}
	return false
}

// InStringArray checks to see if a string is in a []string
func InStringArray(s string, a []string) bool {
	for _, s2 := range a {
		if s2 == s {
			return true
		}
	}
	return false
}

// InStringArraySorted checks to see if a string is in a sorted []string
func InStringArraySorted(s string, a []string) bool {
	idx := sort.Search(len(a), func(i int) bool {
		return a[i] >= s
	})
	return idx < len(a) && a[idx] == s
}

func InsertSortedString(s string, a []string) (a2 []string) {
	idx := sort.Search(len(a), func(i int) bool {
		return a[i] >= s
	})
	a2 = append(a, "")
	copy(a2[idx+1:], a2[idx:])
	a2[idx] = s
	return
}

// BToMB divides the given number by (1024*1024)
func BToMB(b uint64) uint64 {
	return b / (1024 * 1024)
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

// HashPassword hashes a plaintext string password into a format which can be
// inserted into the database.
func HashPassword(p string) string {
	h, err := bcrypt.GenerateFromPassword([]byte(p), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	dbPassword := fmt.Sprintf("bcrypt:%s", base32.StdEncoding.EncodeToString(h))
	return dbPassword
}

// ComparePasswordHash compares the password hash generated by HashPassword()
// with the given plaintext password string.
func ComparePasswordHash(hash, p string) bool {
	if !strings.HasPrefix(hash, "bcrypt:") {
		return false
	}
	h, err := base32.StdEncoding.DecodeString(hash[7:])
	if err != nil {
		return false
	}
	err = bcrypt.CompareHashAndPassword(h, []byte(p))
	if err != nil {
		return false
	}
	return true
}

// Seeds the math.Rand() generator.
func InitRandom() {
	hn, err := os.Hostname()
	s := time.Now().UTC().UnixNano()
	if err == nil {
		s += int64(crc32.ChecksumIEEE([]byte(hn)))
	}
	rand.Seed(s)
}

func SplitStringWords(s string, maxLen int) (result []string) {
	result = []string{}
	curString := ""
	s = strings.TrimSpace(s)
	for s != "" {
		p := strings.IndexByte(s, ' ')
		word := ""
		if p == -1 {
			word = s
			s = ""
		} else {
			word = s[0:p]
			s = s[p+1:]
		}
		if len(curString)+len(word)+1 < maxLen {
			curString = curString + " " + word
		} else {
			if curString != "" {
				result = append(result, strings.TrimSpace(curString))
			}
			curString = word
		}

		s = strings.TrimSpace(s)
	}
	if curString != "" {
		result = append(result, strings.TrimSpace(curString))
	}
	return
}
