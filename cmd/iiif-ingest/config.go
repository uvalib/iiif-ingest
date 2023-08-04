package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var maxNameRegex = 32

// ServiceConfig defines all the service configuration parameters
type ServiceConfig struct {

	// service configuration
	InQueueName     string // SQS queue name for inbound documents
	PollTimeOut     int64  // the SQS queue timeout (in seconds)
	LocalWorkDir    string // the local work directory
	WorkerQueueSize int    // the inbound message queue size to feed the workers
	Workers         int    // the number of worker processes

	// conversion configuration
	ConvertBinary  string // the conversion binary
	ConvertSuffix  string // the suffix of converyed files
	ConvertOptions string // the conversion options
	DeleteSource   bool   // delete the bucket object after conversion

	// output/naming configuration
	OutputRoot         string   // the output root directory
	OutputBucket       string   // the output bucket
	InputNameRegex     []string // the list of possible input name regular expressions
	OutputNameTemplate []string // the list of corresponding output name templates
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

	// service configuration
	cfg.InQueueName = ensureSetAndNonEmpty("IIIF_INGEST_IN_QUEUE")
	cfg.PollTimeOut = int64(envToInt("IIIF_INGEST_QUEUE_POLL_TIMEOUT"))
	cfg.LocalWorkDir = ensureSetAndNonEmpty("IIIF_INGEST_WORK_DIR")
	cfg.WorkerQueueSize = envToInt("IIIF_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("IIIF_INGEST_WORKERS")

	// conversion configuration
	cfg.ConvertBinary = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_BIN")
	cfg.ConvertSuffix = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_SUFFIX")
	cfg.ConvertOptions = ensureSetAndNonEmpty("IIIF_INGEST_CONVERT_OPTS")
	cfg.DeleteSource = envToBoolean("IIIF_INGEST_DELETE_SOURCE")

	// output configuration
	cfg.OutputRoot = envWithDefault("IIIF_INGEST_OUTPUT_ROOT", "")
	cfg.OutputBucket = envWithDefault("IIIF_INGEST_OUTPUT_BUCKET", "")

	for ix := 0; ix < maxNameRegex; ix++ {
		env := fmt.Sprintf("IIIF_INGEST_NAME_MAP_%02d", ix+1)
		val, set := os.LookupEnv(env)
		if set == true {
			s := strings.Split(val, "=")
			if len(s) == 2 {
				// ensure the regex compiles
				_, err := regexp.Compile(strings.TrimSpace(s[0]))
				if err != nil {
					log.Printf("[main] ERROR: incorrectly formatted '%s' value (%s)", env, val)
					os.Exit(1)
				}
				cfg.InputNameRegex = append(cfg.InputNameRegex, strings.TrimSpace(s[0]))
				cfg.OutputNameTemplate = append(cfg.OutputNameTemplate, strings.TrimSpace(s[1]))
			} else {
				log.Printf("[main] ERROR: incorrectly formatted '%s' value (%s)", env, val)
				os.Exit(1)
			}
		} else {
			break
		}
	}

	// service configuration
	log.Printf("[config] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[config] PollTimeOut          = [%d]", cfg.PollTimeOut)
	log.Printf("[config] LocalWorkDir         = [%s]", cfg.LocalWorkDir)
	log.Printf("[config] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize)
	log.Printf("[config] Workers              = [%d]", cfg.Workers)

	// conversion configuration
	log.Printf("[config] ConvertBinary        = [%s]", cfg.ConvertBinary)
	log.Printf("[config] ConvertSuffix        = [%s]", cfg.ConvertSuffix)
	log.Printf("[config] ConvertOptions       = [%s]", cfg.ConvertOptions)
	log.Printf("[config] DeleteSource         = [%t]", cfg.DeleteSource)

	// output configuration
	log.Printf("[config] OutputRoot           = [%s]", cfg.OutputRoot)
	log.Printf("[config] OutputBucket         = [%s]", cfg.OutputBucket)

	for ix, _ := range cfg.InputNameRegex {
		log.Printf("[config] Input name map %02d    = [%s -> %s]", ix+1, cfg.InputNameRegex[ix], cfg.OutputNameTemplate[ix])
	}

	// validate output target values
	if len(cfg.OutputRoot) == 0 && len(cfg.OutputBucket) == 0 {
		log.Printf("[main] ERROR: must specify output root (IIIF_INGEST_OUTPUT_ROOT) or output bucket (IIIF_INGEST_OUTPUT_BUCKET)")
		os.Exit(1)
	}

	if len(cfg.OutputRoot) != 0 && len(cfg.OutputBucket) != 0 {
		log.Printf("[main] ERROR: cannot specify output root (IIIF_INGEST_OUTPUT_ROOT) and output bucket (IIIF_INGEST_OUTPUT_BUCKET)")
		os.Exit(1)
	}

	return &cfg
}
