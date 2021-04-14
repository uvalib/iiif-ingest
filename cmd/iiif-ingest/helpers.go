package main

import "log"

func fatalIfError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

//
// end of file
//
