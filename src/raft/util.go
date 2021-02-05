package raft

import (
	"log"
)

// Debugging
const Debug = 0

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
