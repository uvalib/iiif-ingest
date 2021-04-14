package main

// this describes the structure of the event received from S3

type Events struct {
	Records []S3EventRecord `json:"Records"`
}

type S3EventRecord struct {
	S3 S3Record `json:"S3"`
}

type S3Record struct {
	Bucket BucketRecord `json:"bucket"`
	Object ObjectRecord `json:"object"`
}

type BucketRecord struct {
	Name string `json:"name"`
}

type ObjectRecord struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

//
// end of file
//
