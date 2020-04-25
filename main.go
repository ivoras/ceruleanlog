package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	eventQuit = iota
)

type sysEventMessage struct {
	event int
	idata int
}

var sysEventChannel = make(chan sysEventMessage, 5)

var logFileName = flag.String("log", "/tmp/ceruleanlog.log", "Log file ('-' for only stderr)")
var dataDir = flag.String("data", "./cerulean_data", "Data directory")
var configFile = flag.String("conf", "ceruleanlog.json", "JSON config file")
var logOutput io.Writer
var startTime time.Time
var globalConfig CeruleanConfig

func main() {
	os.Setenv("TZ", "UTC")
	startTime = time.Now()
	if *logFileName != "-" {
		f, err := os.OpenFile(*logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
		if err != nil {
			log.Panic("Cannot open log file " + *logFileName)
		}
		defer f.Close()
		logOutput = io.MultiWriter(os.Stderr, f)
	} else {
		logOutput = os.Stderr
	}
	log.SetOutput(logOutput)

	log.Println("Starting up...")

	flag.Parse()

	initEnvironment()

	InitRandom()

	/*
		if len(flag.Args()) > 0 {
			if processCmdLineActions() {
				return
			}
		}
	*/

	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGINT)

	go webServer()
	go msgBuffer.Committer()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	oldAlloc := int64(m.Alloc)
	printMemStats(&m)

	for {
		select {
		case msg := <-sysEventChannel:
			switch msg.event {
			case eventQuit:
				log.Println("Exiting")
				os.Exit(msg.idata)
			}
		case sig := <-sigChannel:
			switch sig {
			case syscall.SIGINT:
				sysEventChannel <- sysEventMessage{event: eventQuit, idata: 0}
				log.Println("^C detected")
			}
		case <-time.After(60 * time.Second):

			runtime.ReadMemStats(&m)
			if AbsInt64(int64(m.Alloc)-oldAlloc) > 1024*1024 {
				printMemStats(&m)
				oldAlloc = int64(m.Alloc)
			}
		}
	}
}

func printMemStats(m *runtime.MemStats) {
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	log.Printf("Alloc: %v MiB\tTotalAlloc: %v MiB\tSys: %v MiB\tNumGC: %v\tUptime: %0.1fh\n",
		BToMB(m.Alloc), BToMB(m.TotalAlloc), BToMB(m.Sys), m.NumGC, time.Since(startTime).Hours())
}
