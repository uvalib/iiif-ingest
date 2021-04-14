package main

import (
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"os"
	"path"
	"strings"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: " "})
	fatalIfError(err)

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	fatalIfError(err)

	// create the notification channel
	notifyChan := make(chan Notify, cfg.WorkerQueueSize)

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, notifyChan)
	}

	for {
		// top of our processing loop
		err = nil

		// notification that there is one or more new ingest files to be processed
		inbound, receiptHandle, e := getInboundNotification(*cfg, aws, inQueueHandle)
		fatalIfError(e)

		// download each file and validate it
		localNames := make([]string, 0, len(inbound))
		for _, f := range inbound {

			// download the file
			localFile, e := s3download(cfg.DownloadDir, f.SourceBucket, f.SourceKey, f.ObjectSize)
			fatalIfError(e)

			// save the local name, we will need it later
			localNames = append(localNames, localFile)
		}

		// if we got here without an error then all the files are downloaded... we can delete the inbound message
		// because it has been processed

		delMessages := make([]awssqs.Message, 0, 1)
		delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
		opStatus, err := aws.BatchMessageDelete(inQueueHandle, delMessages)
		if err != nil {
			if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
				fatalIfError(err)
			}
		}

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("WARNING: message %d failed to delete", ix)
			}
		}

		// now we can process each of the inbound files
		for ix, f := range inbound {
			baseName := path.Base( f.SourceKey )
			fileExt := path.Ext( baseName )
			convertName := fmt.Sprintf( "%s.%s", strings.TrimSuffix(baseName, fileExt), cfg.ConvertSuffix )
			notify := Notify{
				Bucket:        f.SourceBucket,
				BucketKey:     f.SourceKey,
				LocalFile:     localNames[ix],
			    ConvertedFile: fmt.Sprintf( "%s/%s", cfg.ConvertDir, convertName ) }
			notifyChan <- notify
		}
	}
}

//
// end of file
//
