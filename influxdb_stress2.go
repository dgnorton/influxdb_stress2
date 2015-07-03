package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

var (
	measurements = []string{"cpu", "packets_rx", "packets_tx", "mem", "disk", "connections"}
	countries    = []string{"DEU", "GBR", "FRA", "SWE", "SWZ", "JPN", "USA", "CAN", "MEX"}
	regions      = []string{"nwest", "neast", "seast", "swest"}
	dataCenters  = []string{"data_center_001", "data_center_002", "data_center_003"}
	servers      = []string{}

	batchSize     = flag.Int("batchsize", 5000, "number of points per batch")
	batchInterval = flag.Duration("batchinterval", 0*time.Second, "duration between batches")
	serverCnt     = flag.Int("servercnt", 400, "number of servers per data center (rough control of # of series)")
	stressMetrics = flag.Bool("stressmetrics", true, "if enabled, influxdb_stress2 will write performance metrics")
	database      = flag.String("database", "stress", "name of database")
	resetDatabase = flag.Bool("resetdatabase", false, "deletes database, if it exists, and creates a new one")
	address       = flag.String("addr", "localhost:8086", "IP address and port of database (e.g., localhost:8086)")
)

func init() {
	flag.Parse()

	for i := 0; i < *serverCnt; i++ {
		servers = append(servers, fmt.Sprintf("server%.5d", i))
	}
}

// source runs in a goroutine and generates points on the specifed out channel.
func source(name string, tags map[string]string, points chan *client.Point, stop chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	value := 0.0
	for {
		select {
		case <-stop:
			return
		case <-time.After(1 * time.Microsecond):
			p := &client.Point{
				Measurement: name,
				Tags:        tags,
				Time:        time.Now(),
				Fields:      map[string]interface{}{"value": value},
			}
			value++
			points <- p
		}
	}
}

type responseTime struct {
	time     time.Time
	duration time.Duration
}

func newResponseTime(start, end time.Time) *responseTime {
	return &responseTime{
		time:     time.Now(),
		duration: end.Sub(start),
	}
}

func resetDB(c *client.Client, database string) error {
	_, err := c.Query(client.Query{
		Command: fmt.Sprintf("DROP DATABASE %s", database),
	})

	if err != nil && !strings.Contains(err.Error(), "database not found") {
		return err
	}

	return nil
}

func main() {
	u, _ := url.Parse(fmt.Sprintf("http://%s", *address))
	cfg := client.Config{URL: *u}
	c, err := client.NewClient(cfg)
	checkerr(err)

	if *resetDatabase {
		resetDB(c, *database)
	}

	_, err = c.Query(client.Query{
		Command: fmt.Sprintf("CREATE DATABASE %s", *database),
	})

	if err != nil && !strings.Contains(err.Error(), "database already exists") {
		log.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	points := make(chan *client.Point)
	stop := make(chan struct{})

	fmt.Println("starting data sources")
	seriesCnt := 0
	for _, m := range measurements {
		for _, c := range countries {
			for _, r := range regions {
				for _, d := range dataCenters {
					for _, s := range servers {
						tags := map[string]string{"country": c, "region": r, "data_center": d, "server": s}
						go source(m, tags, points, stop, wg)
						seriesCnt++
					}
				}
			}
		}
	}

	fmt.Printf("writing %d series......\n", seriesCnt)
	total := 0
	responseTimes := []*responseTime{}
	for {
		batch := &client.BatchPoints{
			Points:           make([]client.Point, 0),
			Database:         *database,
			RetentionPolicy:  "default",
			WriteConsistency: "Any",
		}

		for p := range points {
			batch.Points = append(batch.Points, *p)
			if len(batch.Points) == *batchSize {
				break
			}
		}

		start := time.Now()
		_, err := c.Write(*batch)
		end := time.Now()
		checkerr(err)

		if *batchInterval > 0 {
			time.Sleep(*batchInterval)
		}

		responseTimes = append(responseTimes, newResponseTime(start, end))
		if len(responseTimes) > 99 {
			writeResponseTimes(c, responseTimes)
			responseTimes = []*responseTime{}
		}

		total += len(batch.Points)
		fmt.Printf("total points written: %d\n", total)
	}
}

func writeResponseTimes(c *client.Client, rtimes []*responseTime) {
	batch := &client.BatchPoints{
		Points:           make([]client.Point, 0, len(rtimes)),
		Database:         *database,
		RetentionPolicy:  "default",
		WriteConsistency: "Any",
	}

	tags := map[string]string{
		"app": "influxdb_stress2",
	}

	for _, rt := range rtimes {
		p := &client.Point{
			Measurement: "response_time",
			Tags:        tags,
			Time:        rt.time,
			Fields:      map[string]interface{}{"value": rt.duration.Nanoseconds()},
		}
		batch.Points = append(batch.Points, *p)
	}
	_, err := c.Write(*batch)
	checkerr(err)
	fmt.Printf("wrote %d InfluxDB response times\n", len(rtimes))
}

func checkerr(err error) {
	if err != nil {
		if strings.Contains(err.Error(), "timeout") {
			log.Println(err)
		} else {
			log.Fatalln(err)
		}
	}
}
