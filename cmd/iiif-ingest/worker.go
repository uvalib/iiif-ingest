package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Notify - our worker notification structure
type Notify struct {
	Bucket        string  // the name of the bucket
	BucketKey     string  // the bucket key (file name)
	LocalFile     string  // the local file name
	ConvertedFile string  // the converted file
}

func worker(id int, config ServiceConfig, notifies <-chan Notify) {

	var notify Notify
	for {
		// wait for an inbound file
		notify = <-notifies

		log.Printf("INFO: processing %s (%s)", notify.BucketKey, notify.LocalFile )

		// do the conversion
		params := strings.Split( config.ConvertOptions, " " )
		var cmd *exec.Cmd
		switch len( params ) {
		case 0:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, notify.ConvertedFile )
		case 1:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, params[0], notify.ConvertedFile )
		case 2:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, params[0], params[1], notify.ConvertedFile )
		case 3:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, params[0], params[1], params[2], notify.ConvertedFile )
		case 4:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, params[0], params[1], params[2], params[3], notify.ConvertedFile )
		case 5:
			cmd = exec.Command( config.ConvertBinary, notify.LocalFile, params[0], params[1], params[2], params[3], params[4], notify.ConvertedFile )
		default:
			fatalIfError( fmt.Errorf( "excessive command options (%d), update code", len( params ) ))
		}
		log.Printf("DEBUG: CMD \"%s\"", cmd.String() )
		start := time.Now()
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("ERROR: processing %s (%s)", notify.BucketKey, err.Error() )
            if len( output ) != 0 {
				log.Printf("%s", output )
			}
			// remove the converted file, ignore any errors
			_ = os.Remove(notify.ConvertedFile)
		} else {
			duration := time.Since(start)
			log.Printf("INFO: conversion complete in %0.2f seconds", duration.Seconds())
			if config.DeleteAfterConvert == true {
				// bucket file has been processed, remove it
				log.Printf("INFO: removing S3 object %s/%s", notify.Bucket, notify.BucketKey)
				err = s3Delete(notify.Bucket, notify.BucketKey)
				fatalIfError(err)
			}
		}

		// original file has been converted, remove it
		log.Printf("INFO: removing downloaded file %s", notify.LocalFile)
		err = os.Remove(notify.LocalFile)
		fatalIfError(err)
	}

	// should never get here
}

//
// end of file
//
