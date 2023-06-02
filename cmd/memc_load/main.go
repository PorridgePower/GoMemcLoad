package main

import (
	"flag"
	"fmt"
	"memc_load/internal/pkg/parser"
	"os"
	"path/filepath"

	"github.com/bradfitz/gomemcache/memcache"
	log "github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
)

type Options struct {
	Pattern                string
	IDFA, GAID, ADID, DVID string
	Dry, Debug             bool
	LogFile                string
}

func parseCmd() Options {
	var options Options

	flag.StringVar(&options.LogFile, "log", "", "")
	flag.StringVar(&options.Pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "")
	flag.StringVar(&options.IDFA, "idfa", "127.0.0.1:33013", "")
	flag.StringVar(&options.GAID, "gaid", "127.0.0.1:33014", "")
	flag.StringVar(&options.ADID, "adid", "127.0.0.1:33015", "")
	flag.StringVar(&options.DVID, "dvid", "127.0.0.1:33016", "")
	flag.BoolVar(&options.Dry, "dry", false, "")
	flag.BoolVar(&options.Debug, "debug", false, "")
	flag.Parse()

	if options.Pattern == "" {
		log.Fatalf("Pattern must be set")
	}

	if options.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	log.SetFormatter(&easy.Formatter{
		TimestampFormat: "2006.01.02 15:04:05",
		LogFormat:       "[%time%] %lvl% %msg%",
	})

	if options.LogFile != "" {
		logFile, err := os.OpenFile(options.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}

		log.SetOutput(logFile)
	}

	return options
}

func main() {
	options := parseCmd()

	log.Info(fmt.Sprintf("Memc loader started with options: %+v\n", options))

	clients := make(map[string]*memcache.Client)
	clients["idfa"] = memcache.New(options.IDFA)
	clients["gaid"] = memcache.New(options.GAID)
	clients["adid"] = memcache.New(options.ADID)
	clients["dvid"] = memcache.New(options.DVID)

	files, err := filepath.Glob(options.Pattern)
	if err != nil {
		log.Fatalf("No files found for pattern %s\n", options.Pattern)
	}
	log.Infof("Start processing %d files...\n", len(files))

	parser := parser.New(len(files), clients, options.Dry)

	parser.Run(files)
}
