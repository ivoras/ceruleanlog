package logcore

import (
	"fmt"
	"log"
	"os"
)

type CeruleanInstance struct {
	dataDir         string
	configFile      string
	config          CeruleanConfig
	msgBuffer       MsgBuffer
	shardCollection DbShardCollection
}

func (ci CeruleanInstance) getConfigFileName() string {
	return fmt.Sprintf("%s/%s", ci.dataDir, ci.configFile)
}

func (ci CeruleanInstance) getShardsDir() string {
	return fmt.Sprintf("%s/%s", ci.dataDir, "shards")
}

func NewCeruleanInstance(dataDir string) *CeruleanInstance {

	instance := CeruleanInstance{
		dataDir:         dataDir,
		configFile:      "ceruleanlog.json",
		config:          NewCeruleanConfig(),
		msgBuffer:       MsgBuffer{Messages: []BasicGelfMessage{}},
		shardCollection: DbShardCollection{shards: map[uint32]*DbShard{}},
	}
	instance.msgBuffer.instance = &instance

	st, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dataDir, 0755)
		if err != nil {
			log.Panicln(err)
		}

		err = WriteCeruleanConfig(instance.getConfigFileName(), instance.config)
		if err != nil {
			log.Panicln(err)
		}
		err = os.MkdirAll(instance.getShardsDir(), 0755)
		if err != nil {
			log.Panicln(err)
		}
		log.Println("Initialised data directory", dataDir)
	} else if !st.IsDir() {
		log.Panicln("Not a directory:", dataDir)
	}

	if readConfig, err := ReadCeruleanConfig(instance.getConfigFileName()); err == nil {
		instance.config = readConfig
	} else {
		err = WriteCeruleanConfig(instance.getConfigFileName(), instance.config)
	}

	return &instance
}

func (ci *CeruleanInstance) Committer() {
	ci.msgBuffer.committer()
}

func (ci *CeruleanInstance) AddMessage(msg BasicGelfMessage) (err error) {
	return ci.msgBuffer.addMessage(msg)
}
