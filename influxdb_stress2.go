package main

import (
	"fmt"
	"flag"
	"log"
	"math/rand"
	"sync"
	"time"
	"net/url"

	"github.com/influxdb/influxdb/client"
)

var host = flag.String("host", "localhost", "host to connect to")
var port = flag.String("port", "8086", "port to connect to")

var (
	//measurements = []string{"cpu", "packets_rx", "packets_tx", "mem", "disk", "connections"}
	measurements = []Measurement{
		NewCPUMeasurement(),
		NewCounterMeasurement("bytes_tx", 5000000),
		NewCounterMeasurement("bytes_rx", 5000000),
	}
	countries = []string{"DEU", "GBR", "FRA", "SWE", "SWZ", "JPN", "USA", "CAN", "MEX"}
	regions = []string{"nwest", "neast", "seast", "swest"}
	dataCenters = []string{"data_center_001", "data_center_002", "data_center_003"}
	servers = []string{}
)

func init() {
	for i := 0; i < 1000; i++ {
		servers = append(servers, fmt.Sprintf("server%.5d", i))
	}
}

type Measurement interface {
	Name() string
	Value() interface{}
	Interval() time.Duration
}

type CPUMeasurement struct {
	value float64
	norm float64
	spike float64
	interval time.Duration
}

func NewCPUMeasurement() *CPUMeasurement { return &CPUMeasurement{0.15, 0.1, 0.75, time.Microsecond}}

func (m *CPUMeasurement) Name() string { return "cpu" }

func (m *CPUMeasurement) Interval() time.Duration { return m.interval }

func (m *CPUMeasurement) Value() interface{} {
	// Roll the dice.
	v := rand.Float64()

	if m.value > m.spike {
		// 5% chance things go back to normal
		// 5% chance it gets worse
		// 90% chance it remains about the same
		if v < 0.00001 {
			m.value = m.norm + rand.Float64() / 10
		} else if v > 0.99999 {
			m.value = v
		} else {
			m.value = m.spike + rand.Float64() / 10
		}
	} else {
		if v > 0.99999 {
			m.value = m.spike + rand.Float64() / 10
		} else {
			m.value = m.norm + rand.Float64() / 10
		}
	}

	return m.value * 100.0
}

type CounterMeasurement struct {
	name string
	value uint64
	interval time.Duration
	maxPerInterval int
}

func NewCounterMeasurement(name string, maxPerSecond int64) *CounterMeasurement {
	m := &CounterMeasurement{
		name: name,
		interval: time.Second,
	}
	m.maxPerInterval = int(maxPerSecond / (m.interval.Nanoseconds() / 1000000000))

	return m
}

func (m *CounterMeasurement) Name() string { return m.name }

func (m *CounterMeasurement) Interval() time.Duration { return m.interval }

func (m *CounterMeasurement) Value() interface{} {
	old := m.value
	m.value = m.value + uint64(rand.Intn(m.maxPerInterval))
	if old > m.value { panic(fmt.Errorf("%d, %d", old, m.value))}
	return m.value
}

// source runs in a goroutine and generates points on the specifed out channel.
func source(m Measurement, tags map[string]string, points chan *client.Point, stop chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-stop:
			return
		case <-time.After(m.Interval()):
			p := &client.Point{
				Name: m.Name(),
				Tags: tags,
				Timestamp: time.Now(),
				Fields: map[string]interface{}{"value": m.Value()},
			}
			points <-p
		}
	}
}

func main() {
	flag.Parse()

	rand.Seed( time.Now().UTC().UnixNano())
	u, _ := url.Parse(fmt.Sprintf("http://%s:%s", *host, *port))
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
