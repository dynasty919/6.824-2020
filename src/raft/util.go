package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(a...)
	}
	return
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) resetElectionTimer() {
	t := 1000 + rand.Intn(1000)
	rf.electionTimer = time.Duration(t) * time.Millisecond
}
