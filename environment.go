package main

import (
	"fmt"
	"log"
	"os"
)

func initEnvironment() {
	st, err := os.Stat(*dataDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(*dataDir, 0755)
		if err != nil {
			log.Panicln(err)
		}
		globalConfig = NewCeruleanConfig()
		err = WriteCeruleanConfig(getConfigFileName(), globalConfig)
		if err != nil {
			log.Panicln(err)
		}
		err = os.MkdirAll(getShardsDir(), 0755)
		if err != nil {
			log.Panicln(err)
		}
	}
	if !st.IsDir() {
		log.Panicln("Not a directory:", *dataDir)
	}

	globalConfig, err = ReadCeruleanConfig(getConfigFileName())
	if err != nil {
		log.Panicln("Cannot load config file", err)
	}
}

func getShardsDir() string {
	return fmt.Sprintf("%s/%s", *dataDir, "shards")
}
