package main

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"

	influx "github.com/influxdb/influxdb/client"
	metrics "github.com/rcrowley/go-metrics"
)

var deliverySizeHistogram = metrics.GetOrRegisterHistogram("lumbermill.poster.deliver.sizes", metrics.DefaultRegistry, metrics.NewUniformSample(100))

var tokenAppMap = make(map[string]string)

type poster struct {
	destination          *destination
	name                 string
	influxClient         *influx.Client
	pointsSuccessCounter metrics.Counter
	pointsSuccessTime    metrics.Timer
	pointsFailureCounter metrics.Counter
	pointsFailureTime    metrics.Timer
}

func init() {
	s := os.Getenv("APP_TOKENS")
	if s == "" {
		log.Fatal("No environment variable APP_TOKENS")
	}
	pairs := strings.Split(s, "|")
	for _, p := range pairs {
		pair := strings.Split(strings.Trim(p, " "), ":")
		app, token := strings.Trim(pair[0], " "), strings.Trim(pair[1], " ")
		tokenAppMap[token] = app
	}

}

func newPoster(clientConfig influx.Config, name string, destination *destination, waitGroup *sync.WaitGroup) *poster {
	influxClient, err := influx.NewClient(clientConfig)

	if err != nil {
		panic(err)
	}

	return &poster{
		destination:          destination,
		name:                 name,
		influxClient:         influxClient,
		pointsSuccessCounter: metrics.GetOrRegisterCounter("lumbermill.poster.deliver.points."+name, metrics.DefaultRegistry),
		pointsSuccessTime:    metrics.GetOrRegisterTimer("lumbermill.poster.success.time."+name, metrics.DefaultRegistry),
		pointsFailureCounter: metrics.GetOrRegisterCounter("lumbermill.poster.error.points."+name, metrics.DefaultRegistry),
		pointsFailureTime:    metrics.GetOrRegisterTimer("lumbermill.poster.error.time."+name, metrics.DefaultRegistry),
	}
}

func (p *poster) Run() {
	var last bool
	var delivery *influx.BatchPoints

	timeout := time.NewTicker(time.Second)
	defer func() { timeout.Stop() }()

	for !last {
		delivery, last = p.nextDelivery(timeout)
		p.deliver(delivery)
	}
}

func (p *poster) nextDelivery(timeout *time.Ticker) (delivery *influx.BatchPoints, last bool) {
	delivery = &influx.BatchPoints{
		Points:          []influx.Point{},
		Database:        DBName,
		RetentionPolicy: "default",
	}
	for {
		select {
		case point, open := <-p.destination.points:
			if !open {
				return delivery, true
			}
			app, in := tokenAppMap[point.Token]
			if !in {
				log.Printf("Token %s has no associated application\n", point.Token)
			}
			fields := make(map[string]interface{})
			tags := make(map[string]string)
			tagNames := point.Type.TagColumns()
			fieldNames := point.Type.fieldColumns()
			for i := 1; i < len(point.Tags); i++ {
				tags[tagNames[i]] = point.Tags[i]
			}
			tags["app"] = app

			for i := 1; i < len(point.Fields); i++ {
				fields[fieldNames[i]] = point.Fields[i]
			}

			p := influx.Point{
				Measurement: point.Type.Name(),
				Tags:        tags,
				Fields:      fields,
				Time:        point.Time,
			}
			delivery.Points = append(delivery.Points, p)
		case <-timeout.C:
			return delivery, false
		}
	}
}

func (p *poster) deliver(bps *influx.BatchPoints) {
	start := time.Now()
	_, err := p.influxClient.Write(*bps)

	if err != nil {
		// TODO: Ugh. These could be timeout errors, or an internal error.
		//       Should probably attempt to figure out which...
		p.pointsFailureCounter.Inc(1)
		p.pointsFailureTime.UpdateSince(start)
		log.Printf("Error posting points: %s\n", err)
	} else {
		p.pointsSuccessCounter.Inc(1)
		p.pointsSuccessTime.UpdateSince(start)
		deliverySizeHistogram.Update(int64(len(bps.Points)))
	}
}
