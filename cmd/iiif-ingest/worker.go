package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// Notify - our worker notification structure
type Notify struct {
	SourceBucket  string               // the bucket name
	BucketKey     string               // the bucket key (file name)
	ExpectedSize  int64                // the expected size of the object
	ReceiptHandle awssqs.ReceiptHandle // the inbound message receipt handle (so we can delete it)
}

func worker(workerId int, config ServiceConfig, sqsSvc awssqs.AWS_SQS, s3Svc uva_s3.UvaS3, queue awssqs.QueueHandle, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		start := time.Now()
		log.Printf("[worker %d] INFO: begin processing %s", workerId, notify.BucketKey)

		// validate the inbound file naming convention
		err := validateInputName(workerId, config, notify.BucketKey)
		if err != nil {
			log.Printf("[worker %d] ERROR: input name %s is invalid (%s)", workerId, notify.BucketKey, err.Error())
			continue
		}

		// create the output file name
		outputFile := generateOutputName(workerId, config, notify.BucketKey)

		// create the target directory tree if we are outputting to a local filesystem
		if len(config.OutputFSRoot) != 0 {
			fullOutputFile := fmt.Sprintf("%s/%s", config.OutputFSRoot, outputFile)
			err = createOutputDirectory(workerId, fullOutputFile)
			if err != nil {
				continue
			}
		}

		// create temp file
		tmp, err := ioutil.TempFile(config.LocalWorkDir, "")
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to create temp file (%s)", workerId, err.Error())
			continue
		}

		tmp.Close()
		downloadFile := tmp.Name()

		// download the file
		o := uva_s3.NewUvaS3Object(notify.SourceBucket, notify.BucketKey)
		err = s3Svc.GetToFile(o, downloadFile)
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to download %s (%s)", workerId, notify.BucketKey, err.Error())
			continue
		}

		// convert the file
		workFile, err := convertFile(workerId, config, notify.BucketKey, downloadFile)
		if err != nil {
			continue
		}

		// if we are outputting to a local filesystem
		if len(config.OutputFSRoot) != 0 {
			fullOutputFile := fmt.Sprintf("%s/%s", config.OutputFSRoot, outputFile)
			// copy the file to the correct location and delete the original
			err = copyFile(workerId, workFile, fullOutputFile)
			_ = os.Remove(workFile)
			if err != nil {
				log.Printf("[worker %d] ERROR: failed to copy %s to %s (%s)", workerId, workFile, outputFile, err.Error())
				continue
			}
		} else {
			// we are outputting to a bucket
			o := uva_s3.NewUvaS3Object(config.OutputBucket, outputFile)
			err := s3Svc.PutFromFile(o, workFile)
			_ = os.Remove(workFile)
			if err != nil {
				log.Printf("[worker %d] ERROR: failed to upload %s to s3://%s/%s (%s)", workerId, workFile, config.OutputBucket, outputFile, err.Error())
				continue
			}
		}

		// should we delete the bucket contents
		if config.DeleteSource == true {
			// bucket file has been processed, remove it
			log.Printf("[worker %d] INFO: removing S3 object %s/%s", workerId, notify.SourceBucket, notify.BucketKey)
			err = s3Svc.DeleteObject(o)
			if err != nil {
				continue
			}
		}

		// delete the inbound message
		err = deleteMessage(workerId, sqsSvc, queue, notify.ReceiptHandle)
		if err != nil {
			log.Printf("[worker %d] ERROR: failed to delete a processed message (%s)", workerId, err.Error())
			continue
		}

		duration := time.Since(start)
		log.Printf("[worker %d] INFO: processing %s complete in %0.2f seconds", workerId, notify.BucketKey, duration.Seconds())
	}

	// should never get here
}

func convertFile(workerId int, config ServiceConfig, bucketKey string, inputFile string) (string, error) {

	// create a temp file
	f, err := ioutil.TempFile(config.LocalWorkDir, fmt.Sprintf("*.%s", config.ConvertSuffix))
	if err != nil {
		return "", err
	}
	_ = f.Close()
	outputFile := f.Name()

	// do the conversion
	params := strings.Split(config.ConvertOptions, " ")
	var cmd *exec.Cmd
	switch len(params) {
	case 0:
		cmd = exec.Command(config.ConvertBinary, inputFile, outputFile)
	case 1:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], outputFile)
	case 2:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], outputFile)
	case 3:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], outputFile)
	case 4:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], outputFile)
	case 5:
		cmd = exec.Command(config.ConvertBinary, inputFile, params[0], params[1], params[2], params[3], params[4], outputFile)
	default:
		fatalIfError(fmt.Errorf("excessive command options (%d), update code", len(params)))
	}
	log.Printf("[worker %d] DEBUG: convert command \"%s\"", workerId, cmd.String())
	start := time.Now()
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("[worker %d] ERROR: processing %s (%s)", workerId, bucketKey, err.Error())
		if len(output) != 0 {
			log.Printf("[worker %d] ERROR: conversion output [%s]", workerId, output)
		}
		// remove the input and output files and ignore any errors
		_ = os.Remove(inputFile)
		_ = os.Remove(outputFile)

		// return the error
		return "", err
	}

	// cleanup and return
	duration := time.Since(start)
	log.Printf("[worker %d] INFO: conversion complete in %0.2f seconds", workerId, duration.Seconds())

	// if we have some output, log it
	if len(output) != 0 {
		log.Printf("[worker %d] DEBUG: conversion output [%s]", workerId, output)
	}

	// original file has been converted, remove it and ignore any errors
	log.Printf("[worker %d] INFO: removing downloaded file %s", workerId, inputFile)
	_ = os.Remove(inputFile)

	// all good
	return outputFile, nil
}

func deleteMessage(workerId int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, receiptHandle awssqs.ReceiptHandle) error {

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
