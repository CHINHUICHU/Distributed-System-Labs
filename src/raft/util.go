package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// sendRPC runs fn in a background goroutine and waits up to timeout for it to complete.
// Returns true if fn finished within the timeout, false if it timed out.
func sendRPC(fn func(), timeout time.Duration) bool {
	done := make(chan struct{}, 1)
	go func() {
		fn()
		done <- struct{}{}
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

var debugEnabled = os.Getenv("RAFT_DEBUG") != ""

// Debugging
const Debug = false

var (
	start = time.Now()
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Timestamp() int64 {
	return time.Since(start).Abs().Milliseconds()
}

// Log writes diagnostic messages only when RAFT_DEBUG env var is set.
// Use: go test -run 2A                    (clean output, no logs)
//      RAFT_DEBUG=1 go test -run 2A       (verbose with all log messages)
func Log(format string, a ...interface{}) {
	if debugEnabled {
		fmt.Printf(format, a...)
	}
}
