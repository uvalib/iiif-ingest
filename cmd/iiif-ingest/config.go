package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName        string // SQS queue name for inbound documents
	PollTimeOut        int64  // the SQS queue timeout (in seconds)
	LocalWorkDir       string // the local work directory
	WorkerQueueSize    int    // the inbound message queue size to feed the workers
	Workers            int    // the number of worker processes
	ConvertBinary      string // the conversion binary
	ConvertSuffix      string // the suffix of converyed files
	ConvertOptions     string // the conversion options
	ConvertDir         string // the conversion directory
	DeleteAfterConvert bool   // delete the bucket object after conversion
	FailOnOverwrite    bool   // fail if the converted file will overwrite an existing one
}

func envWithDefault(env string, defaultValue string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("[main] INFO: environment variable not set: [%s] using default value [%s]", env, defaultValue)
		return defaultValue
	}

	return val
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("[main] ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("[main] ERROR: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {

	number := ensureSetAndNonEmpty(env)
	n, err := strconv.Atoi(number)
	fatalIfError(err)
	return n
}

func envToBoolean(env string) bool {

	value := ensureSetAndNonEmpty(env)
	b, err := strconv.ParseBool(value)
	fatalIfError(err)
	return b
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty("IIIF_INGEST_IN_QUEUE")
	cfg.PollTimeOut = int64(envToInt("IIIF_INGEST_QUEUE_POLL_TIMEOUT"))
	cfg.LocalWorkDir = ensureSetAndNonEmpty("IIIF_INGEST_WORK_DIR")
	cfg.WorkerQueueSize = envToInt("IIIF_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("IIIF_INGEST_WORKERS")

	// conversion configuration
	cfg.ConvertBinary      = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_BIN")
	cfg.ConvertSuffix      = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_SUFFIX")
	cfg.ConvertOptions     = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_OPTS")
	cfg.ConvertDir         = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_DIR")
    cfg.DeleteAfterConvert = envToBoolean("IIIF_INGEST_DELETE_AFTER_CONVERT")
	cfg.FailOnOverwrite    = envToBoolean("IIIF_INGEST_FAIL_ON_OVERWRITE")

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] LocalWorkDir         = [%s]", cfg.LocalWorkDir)
	log.Printf("[CONFIG] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers)

	log.Printf("[CONFIG] ConvertBinary        = [%s]", cfg.ConvertBinary)
	log.Printf("[CONFIG] ConvertSuffix        = [%s]", cfg.ConvertSuffix)
	log.Printf("[CONFIG] ConvertOptions       = [%s]", cfg.ConvertOptions)
	log.Printf("[CONFIG] ConvertDir           = [%s]", cfg.ConvertDir)
	log.Printf("[CONFIG] DeleteAfterConvert   = [%t]", cfg.DeleteAfterConvert)
	log.Printf("[CONFIG] FailOnOverwrite      = [%t]", cfg.FailOnOverwrite)

	return &cfg
}
