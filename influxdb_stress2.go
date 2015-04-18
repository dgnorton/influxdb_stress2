package main

import (
	"fmt"
	"log"
	"sync"
	"time"
	"net/url"

	"github.com/influxdb/influxdb/client"
)

var (
	measurements = []string{"cpu", "packets_rx", "packets_tx", "mem", "disk", "connections"}
	countries = []string{"DEU", "GBR", "FRA", "SWE", "SWZ", "JPN", "USA", "CAN", "MEX"}
	regions = []string{"nwest", "neast", "seast", "swest"}
	dataCenters = []string{"data_center_001", "data_center_002", "data_center_003"}
	servers = []string{}
)

func init() {
	for i := 0; i < 10; i++ {
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
				Name: name,
				Tags: tags,
				Timestamp: time.Now(),
				Fields: map[string]interface{}{"value": value},
			}
			value++
			points <-p
		}
	}
}

func main() {
	u, _ := url.Parse("http://localhost:8086")
	cfg := client.Config{URL: *u}
	c, err := client.NewClient(cfg)
	checkerr(err)
	
	wg := &sync.WaitGroup{}
	points := make(chan *client.Point)
	stop := make(chan struct{})

	seriesCnt := 0
	for _, m := range measurements {
		for _, c := range countries {
			for _, r := range regions{
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

	fmt.Printf("preparing to write %d series\n", seriesCnt)
	<-time.After(2 * time.Second)
	fmt.Println("writing !!!! ...............")
	total := 0
	for {
		batch := &client.BatchPoints{
			Points: make([]client.Point, 0),
			Database: "foo",
			RetentionPolicy: "default",
		}

		for p := range points {
			batch.Points = append(batch.Points, *p)
			if len(batch.Points) == 5000 {
				break
			}
		}

		_, err := c.Write(*batch)
		checkerr(err)
		total += len(batch.Points)
		fmt.Printf("total points written: %d\n", total)
	}
}

func checkerr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}
