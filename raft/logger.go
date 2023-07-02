package raft

import (
	"log"
	"os"
	"strconv"
	"time"
)

var verbosity int

func getVerbosity() int {
	verbose := os.Getenv("VERBOSE")
	level := 0
	if verbose != "" {
		var err error
		level, err = strconv.Atoi(verbose)
		if err != nil {
			panic("invalid verbosity")
		}
	}
	return level
}

func logf(format string, para ...interface{}) {
	if verbosity == 1 {
		time := time.Now().Format("2006-01-02 15:04:05")
		log.Printf(time + " " + format, para...)
	}
}

func init() {
	verbosity = getVerbosity()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}