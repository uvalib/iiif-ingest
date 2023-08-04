package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
)

func fatalIfError(err error) {
	if err != nil {
		log.Fatalf("FATAL ERROR: %s", err.Error())
	}
}

// validate input file name against the set of valid input regex expressions
func validateInputName(workerId int, config ServiceConfig, inputName string) error {

	log.Printf("[worker %d] DEBUG: validating input name '%s'", workerId, inputName)

	// remove the file suffix
	fileExt := path.Ext(inputName)
	name := strings.TrimSuffix(inputName, fileExt)

	// match against the set of valid input regex expressions
	matched := false
	for ix, _ := range config.InputNameRegex {
		matched, _ = regexp.MatchString(config.InputNameRegex[ix], name)
		if matched == true {
			break
		}
	}

	if matched == false {
		return fmt.Errorf("input filename is invalid (%s)", name)
	}

	// all is well
	return nil
}

// generate the output file name based on the input file and configuration
func generateOutputName(workerId int, config ServiceConfig, inputName string) string {

	log.Printf("[worker %d] DEBUG: generating output name for '%s'", workerId, inputName)

	// remove the file suffix
	fileExt := path.Ext(inputName)
	name := strings.TrimSuffix(inputName, fileExt)

	for ix, _ := range config.InputNameRegex {

		// we have already compiled it during the config phase so ignore the error return
		re, _ := regexp.Compile(config.InputNameRegex[ix])
		//log.Printf("[worker %d] DEBUG: matching [%s]", workerId, config.InputNameRegex[ix])
		// does it match?
		if re.MatchString(name) == true {
			//log.Printf("[worker %d] DEBUG: matched, extracting substrings", workerId)
			outputName := config.OutputNameTemplate[ix]
			asm := re.FindAllStringSubmatch(name, -1)
			for iy, sm := range asm[0] {
				// ignore the 0 index as this is the full string match
				if iy == 0 {
					continue
				}
				//log.Printf("[worker %d] DEBUG: %d = %v", workerId, iy, sm)
				placeholder := fmt.Sprintf("{:%d}", iy)
				outputName = strings.Replace(outputName, placeholder, sm, -1)
			}
			outputName = fmt.Sprintf("%s.%s", outputName, config.ConvertSuffix)
			log.Printf("[worker %d] DEBUG: generated output name [%s] -> [%s]", workerId, inputName, outputName)
			return outputName
		}
	}

	log.Printf("[worker %d] ERROR: generating output name for %s", workerId, inputName)
	// placeholder bogus value
	return "unknown"
}

// create the output directory
func createOutputDirectory(workerId int, outputName string) error {

	// split into path and filename components
	dirName := path.Dir(outputName)

	log.Printf("[worker %d] DEBUG: creating directory %s", workerId, dirName)

	// create the directory if appropriate
	err := os.MkdirAll(dirName, 0755)
	if err != nil {
		log.Printf("[worker %d] ERROR: failed to create output directory %s (%s)", workerId, dirName, err.Error())
		return err
	}

	return nil
}

// copy the file from the old location to the new one... we cannot use os.Rename as this only works withing a
// single device
func copyFile(workerId int, oldLocation, newLocation string) error {

	log.Printf("[worker %d] INFO: copying %s to %s", workerId, oldLocation, newLocation)

	i, err := os.Open(oldLocation)
	if err != nil {
		return err
	}
	defer i.Close()
	o, err := os.Create(newLocation)
	if err != nil {
		return err
	}
	defer o.Close()
	_, err = o.ReadFrom(i)
	return err
}

//
// end of file
//
