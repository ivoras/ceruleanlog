package logcore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type CeruleanInstance struct {
	dataDir         string
	configFile      string
	config          CeruleanConfig
	msgBuffer       MsgBuffer
	shardCollection DbShardCollection
	earliestTime    uint32
}

func (ci CeruleanInstance) getConfigFileName() string {
	return fmt.Sprintf("%s/%s", ci.dataDir, ci.configFile)
}

func (ci CeruleanInstance) getShardsDir() string {
	return fmt.Sprintf("%s/%s", ci.dataDir, "shards")
}

func NewCeruleanInstance(dataDir string) *CeruleanInstance {

	instance := CeruleanInstance{
		dataDir:    dataDir,
		configFile: "ceruleanlog.json",
		config:     NewCeruleanConfig(),
	}
	instance.msgBuffer = NewMsgBuffer(&instance)
	instance.shardCollection = NewDbShardCollection(&instance)

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

func (ci *CeruleanInstance) getShardsInDir() (shards []spanNameID, err error) {
	files, err := ioutil.ReadDir(ci.getShardsDir())
	if err != nil {
		return nil, err
	}
	shards = []spanNameID{}
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
	}
}

func (ci *CeruleanInstance) Committer() {
	ci.msgBuffer.committer()
}

func (ci *CeruleanInstance) AddMessage(msg BasicGelfMessage) (err error) {
	return ci.msgBuffer.addMessage(msg)
}

func (ci *CeruleanInstance) Query(timeFrom, timeTo uint32, query string) {

}
