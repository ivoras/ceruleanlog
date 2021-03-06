package logcore

import (
	"fmt"
	"log"
	"time"
)

// Data flows like this:
// - There's a "current buffer"
// - Incoming messages either go into the current buffer, or directly into shards if so configured
// - There's a goroutine which periodically creates a new current buffer, and commits the data from the old one to the shards

type MsgBuffer struct {
	WithMutex
	Messages     []BasicGelfMessage
	LastSwapTime time.Time
	instance     *CeruleanInstance
}

func NewMsgBuffer(i *CeruleanInstance) (mb MsgBuffer) {
	return MsgBuffer{
		Messages: []BasicGelfMessage{},
		instance: i,
	}
}

func (b *MsgBuffer) addMessage(msg BasicGelfMessage) (err error) {
	//log.Println("CeruleanLog recording message:", jsonifyWhatever(msg))
	b.WithLock(func() {
		b.Messages = append(b.Messages, msg)
		if b.instance.config.MemoryBufferTimeSeconds == 0 {
			oldMessages := b.Messages
			err = b.commitMessagesToShards(&oldMessages)
			if err == nil {
				b.Messages = []BasicGelfMessage{}
			} else {
				log.Println("Error committing message(s) to database shards. Will retry because memory_buffer_time_seconds==0:", err)
			}
		}
	})
	return
}

func (b *MsgBuffer) committer() {
	log.Println(fmt.Sprintf("Starting CeruleanLog committer for %s, flush time %ds.", b.instance.dataDir, b.instance.config.MemoryBufferTimeSeconds))
	for {
		if time.Since(b.LastSwapTime) >= time.Duration(b.instance.config.MemoryBufferTimeSeconds)*time.Second && len(b.Messages) != 0 {
			var oldMessages []BasicGelfMessage
			b.WithLock(func() {
				oldMessages = b.Messages
				b.Messages = []BasicGelfMessage{}
				b.LastSwapTime = time.Now()
			})
			err := b.commitMessagesToShards(&oldMessages)
			if err != nil {
				log.Printf("Cannot commit messages to database shards! %d messages lost! %v", len(oldMessages), err)
			} else {
				log.Printf("CeruleanLog committed %d messages to database shards.", len(oldMessages))
			}
		}
		time.Sleep(1 * time.Second)
	}
	log.Println("Exiting CeruleanLog committer for", b.instance.dataDir)
}

func (b *MsgBuffer) commitMessagesToShards(messages *[]BasicGelfMessage) (err error) {
	now := uint32(getNowUTC())
	for i := range *messages {
		if (*messages)[i].Timestamp == 0 {
			(*messages)[i].Timestamp = now
		}
	}
	err = b.instance.shardCollection.CommitMessagesToShards(messages)
	return
}
