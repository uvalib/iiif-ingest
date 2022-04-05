package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"
)

var downloader *s3manager.Downloader
var s3service *s3.S3

// set up our S3 management objects
func init() {

	sess, err := session.NewSession()
	if err == nil {
		downloader = s3manager.NewDownloader(sess)
		s3service = s3.New(sess)
	}
}

// taken from https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_download_object.go

func s3download(workerId int, downloadDir string, bucket string, object string, expectedSize int64) (string, error) {

	file, err := ioutil.TempFile(downloadDir, fmt.Sprintf("*%s", path.Ext(object)))
	if err != nil {
		return "", err
	}
	defer file.Close()

	sourcename := fmt.Sprintf("s3:/%s/%s", bucket, object)
	log.Printf("[worker %d] INFO: downloading %s to %s", workerId, sourcename, file.Name())

	start := time.Now()
	fileSize, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		})

	if err != nil {
		// remove the file and ignore errors
		_ = os.Remove(file.Name())
		return "", err
	}

	// I think there are times when the download runs out of space but it is not reported as an error so
	// we validate the expected file size against the actually downloaded size
	if expectedSize != fileSize {

		// remove the file and ignore errors
		_ = os.Remove(file.Name())
		return "", fmt.Errorf("download failure. expected %d bytes, received %d bytes", expectedSize, fileSize)
	}

	duration := time.Since(start)
	log.Printf("[worker %d] INFO: download of %s complete in %0.2f seconds (%d bytes)", workerId, sourcename, duration.Seconds(), fileSize)
	return file.Name(), nil
}

// delete the specified S3 object
func s3Delete(workerId int, bucket string, key string) error {

	log.Printf("[worker %d] INFO: deleting s3://%s/%s", workerId, bucket, key)

	start := time.Now()
	_, err := s3service.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		log.Printf("[worker %d] ERROR: deleting s3://%s/%s (%s)", workerId, bucket, key, err.Error())
		return err
	}

	duration := time.Since(start)
	log.Printf("[worker %d] INFO: delete of s3://%s/%s complete in %0.2f seconds", workerId, bucket, key, duration.Seconds())
	return nil
}

//
// end of file
//
