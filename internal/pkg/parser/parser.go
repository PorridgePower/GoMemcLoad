package parser

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	app "memc_load/internal/pkg/parser/user_apps"
	"time"

	pb "memc_load/pkg/api"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	MemcacheMaxRetries = 5
	MemcacheTimeout    = 5
	NormalErrRate      = 0.01
)

var (
	ErrMemcacheConnect = errors.New("memcache connection failed")
)

type Interface interface {
	Run(files []string)
}

func New(numJobs int, clients map[string]*memcache.Client, dry bool) Interface {
	return &Parser{
		clients: clients,
		numJobs: numJobs,
		dry:     dry,
	}
}

type Job struct {
	File  string
	Index int
}

type Parser struct {
	clients map[string]*memcache.Client
	numJobs int
	dry     bool
}

func (p *Parser) insert(userApps app.UserApps) error {
	pbUserApps := &pb.UserApps{
		Lon:  userApps.Lon,
		Lat:  userApps.Lat,
		Apps: userApps.Apps,
	}

	key := fmt.Sprintf("%s:%s", userApps.DevType, userApps.DevId)

	if p.dry {
		messageJson := protojson.Format(pbUserApps)
		log.Debugf("%s - %s -> %s\n", userApps.DevType, key, messageJson)
		return nil
	}

	message, err := proto.Marshal(pbUserApps)
	if err != nil {
		log.Errorf("Couldn`t serialize: %+v\n", pbUserApps)
		return err
	}

	client, ok := p.clients[userApps.DevType]
	if !ok {
		log.Errorf("Invalid device type: %s\n", userApps.DevType)
	}

	item := memcache.Item{Key: key, Value: message}
	for i := 0; i < MemcacheMaxRetries; i++ {
		err := client.Set(&item)
		if err != nil {
			log.Debugf("Failed inserting in memcache: %s\n", userApps.DevType)
			time.Sleep(MemcacheTimeout)
			continue
		}
		return nil
	}

	log.Errorf("Failed to connect to memcache: %s\n", userApps.DevType)
	return ErrMemcacheConnect
}

func (p *Parser) processFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	var processed, errors float64 = 0.0, 0.0

	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		line := scanner.Text()
		processed++

		userApps, err := app.Parse(line)
		if err != nil {
			log.Errorf("Error %s at parsing line %s\n", err, line)
			errors++
			continue
		}

		err = p.insert(userApps)
		if err != nil {
			errors++
		}
	}

	err_rate := errors / processed
	if err_rate < NormalErrRate {
		log.Infof("Acceptable error rate (%f). Successfull load\n", err_rate)
	} else {
		log.Errorf("High error rate (%f > %f). Failed load\n", err_rate, NormalErrRate)
	}
}

func (p *Parser) worker(id int, jobs <-chan Job, results chan<- string) {
	for j := range jobs {
		p.processFile(j.File)
		results <- j.File
	}
}

func (p *Parser) Run(files []string) {
	jobs := make(chan Job, p.numJobs)
	results := make(chan string, p.numJobs)

	for i := 0; i < p.numJobs; i++ {
		go p.worker(i, jobs, results)
		jobs <- Job{files[i], i}
	}
	close(jobs)

	for i := 0; i < p.numJobs; i++ {
		pFile := <-results
		dotRename(pFile)
	}
	close(results)
}

func dotRename(path string) {
	head, fn := filepath.Split(path)

	dotName := filepath.Join(head, "."+fn)
	err := os.Rename(path, dotName)
	if err != nil {
		log.Errorf("Error renaming file %s\n", path)
	}
	log.Debugf("File renamed: %s -> %s\n", path, dotName)
}
