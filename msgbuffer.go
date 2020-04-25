package main

import (
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
}

var msgBuffer = MsgBuffer{Messages: []BasicGelfMessage{}}

func (b *MsgBuffer) AddMessage(msg BasicGelfMessage) (err error) {
	b.WithLock(func() {
		b.Messages = append(b.Messages, msg)
		if globalConfig.MemoryBufferTimeSeconds == 0 {
			oldMessages := b.Messages
			err = b.CommitMessagesToShards(oldMessages)
			if err == nil {
				b.Messages = []BasicGelfMessage{}
			} else {
				log.Println("Error committing message(s) to database shards. Will retry because memory_buffer_time_seconds==0:", err)
			}
		}
	})
	return
}

func (b *MsgBuffer) Committer() {
	for {
		if time.Since(b.LastSwapTime) >= time.Duration(globalConfig.MemoryBufferTimeSeconds)*time.Second {
			var oldMessages []BasicGelfMessage
			b.WithLock(func() {
				oldMessages = b.Messages
				b.Messages = []BasicGelfMessage{}
				b.LastSwapTime = time.Now()
			})
			err := b.CommitMessagesToShards(oldMessages)
			if err != nil {
				log.Printf("Cannot commit messages to database shards! %d messages lost!", len(oldMessages))
			}
		}
	}
}

func (b *MsgBuffer) CommitMessagesToShards(messages []BasicGelfMessage) (err error) {
	for _, msg := range messages {
		err = CommitMessageToShards(msg)
		if err != nil {
			return
		}
	}
	return
}
