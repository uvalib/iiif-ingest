package main

import (
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

// Notify - our worker notification structure
type Notify struct {
	SourceBucket  string                  // the bucket name
	BucketKey     string                  // the bucket key (file name)
	ExpectedSize  int64                   // the expected size of the object
	ReceiptHandle awssqs.ReceiptHandle    // the inbound message receipt handle (so we can delete it)
}

func worker(workerId int, config ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		log.Printf("[worker %d] INFO: processing %s", workerId, notify.BucketKey )

		// download the file
		localFile, err := s3download(workerId, config.DownloadDir, notify.SourceBucket, notify.BucketKey, notify.ExpectedSize)
		fatalIfError(err)

		// create converted filename
		baseName := path.Base( notify.BucketKey )
		fileExt := path.Ext( baseName )
		convertName := fmt.Sprintf( "%s.%s", strings.TrimSuffix(baseName, fileExt), config.ConvertSuffix )
	    outputFile := fmt.Sprintf( "%s/%s", config.ConvertDir, convertName )

	    // convert the file
	    err = convertFile(workerId, config, notify.BucketKey, localFile, outputFile )
		fatalIfError(err)

	    // should we delete the bucket contents
		if config.DeleteAfterConvert == true {
			// bucket file has been processed, remove it
			log.Printf("[worker %d] INFO: removing S3 object %s/%s", workerId, notify.SourceBucket, notify.BucketKey)
			err = s3Delete(workerId, notify.SourceBucket, notify.BucketKey)
			fatalIfError(err)
		}

		// delete the inbound message
		err = deleteMessage(workerId, aws, queue, notify.ReceiptHandle )
		fatalIfError(err)
	}

	// should never get here
}

func convertFile(workerId int, config ServiceConfig, bucketKey string, inputFile string, outputFile string ) error {

	// do the conversion
	params := strings.Split( config.ConvertOptions, " " )
	var cmd *exec.Cmd
	switch len( params ) {
	case 0:
		cmd = exec.Command( config.ConvertBinary, inputFile, outputFile )
	case 1:
		cmd = exec.Command( config.ConvertBinary, inputFile, params[0], outputFile )
	case 2:
		cmd = exec.Command( config.ConvertBinary, inputFile, params[0], params[1], outputFile )
	case 3:
		cmd = exec.Command( config.ConvertBinary, inputFile, params[0], params[1], params[2], outputFile )
	case 4:
		cmd = exec.Command( config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], outputFile )
	case 5:
		cmd = exec.Command( config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], params[4], outputFile )
	default:
		fatalIfError( fmt.Errorf( "excessive command options (%d), update code", len( params ) ))
	}
	log.Printf("[worker %d] DEBUG: convert command \"%s\"", workerId, cmd.String() )
	start := time.Now()
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[worker %d] ERROR: processing %s (%s)", workerId, bucketKey, err.Error() )
		if len( output ) != 0 {
			log.Printf("[worker %d] %s", workerId, output )
		}
		// remove the output file and ignore any errors
		_ = os.Remove(outputFile)

		// return the error
		return err
	}

	// cleanup and return
	duration := time.Since(start)
	log.Printf("[worker %d] INFO: conversion complete in %0.2f seconds", workerId, duration.Seconds())

	// original file has been converted, remove it and ignore any errors
	log.Printf("[worker %d] INFO: removing downloaded file %s", workerId, inputFile)
	_ = os.Remove(inputFile)

	// all good
	return nil
}

func deleteMessage( workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, receiptHandle awssqs.ReceiptHandle ) error {

	log.Printf("[worker %d] INFO: deleting queue message", workerId)

	delMessages := make([]awssqs.Message, 0, 1)
	delMessages = append(delMessages, awssqs.Message{ReceiptHandle: receiptHandle})
	opStatus, err := aws.BatchMessageDelete(queue, delMessages)
	if err != nil {
		if err != awssqs.ErrOneOrMoreOperationsUnsuccessful {
			return err
		}
	}

	// check the operation results
	for ix, op := range opStatus {
		if op == false {
			log.Printf("[worker %d] WARNING: message %d failed to delete", workerId, ix)
		}
	}

	// basically everything OK
	return nil
}

//
// end of file
//
