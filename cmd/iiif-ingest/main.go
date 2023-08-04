package main

import (
	"log"
	"os"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// main entry point
func main() {

	log.Printf("[main] ===> %s service staring up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS sqs helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: " "})
	fatalIfError(err)

	// load our AWS s3 helper object
	s3Svc, err := uva_s3.NewUvaS3(uva_s3.UvaS3Config{Logging: true})
	fatalIfError(err)

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	fatalIfError(err)

	// create the notification channel
	notifyChan := make(chan Notify, cfg.WorkerQueueSize)

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, aws, s3Svc, inQueueHandle, notifyChan)
	}

	for {
		// notification that there is one or more new ingest files to be processed
		inbound, receiptHandle, err := getInboundNotification(*cfg, aws, inQueueHandle)
		fatalIfError(err)

		// create the notification structure and send to the worker queue
		notify := Notify{
			SourceBucket:  inbound.SourceBucket,
			BucketKey:     inbound.SourceKey,
			ExpectedSize:  inbound.ObjectSize,
			ReceiptHandle: receiptHandle,
		}
		notifyChan <- notify
	}

	// should never get here
}

//
// end of file
//
