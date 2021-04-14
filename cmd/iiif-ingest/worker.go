package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"os"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

// Notify - our worker notification structure
type Notify struct {
	Bucket    string  // the name of the bucket
	BucketKey string  // the bucket key (file name)
	LocalFile string  // the local file name
}

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		log.Printf("INFO: processing %s (%s)", notify.BucketKey, notify.LocalFile )

		// do processing here

		// local file has been processed, remove it
		log.Printf("INFO: removing processed file %s", notify.LocalFile)
		err := os.Remove(notify.LocalFile)
		fatalIfError(err)

		// bucket file has been processed, remove it
	}

	// should never get here
}

//
// end of file
//
