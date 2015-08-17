package main

import (
	"log"
	"sync"
	"time"

	influx "github.com/influxdb/influxdb/client"
	metrics "github.com/rcrowley/go-metrics"
)

var deliverySizeHistogram = metrics.GetOrRegisterHistogram("lumbermill.poster.deliver.sizes", metrics.DefaultRegistry, metrics.NewUniformSample(100))

type poster struct {
	destination          *destination
	name                 string
	influxClient         *influx.Client
	pointsSuccessCounter metrics.Counter
	pointsSuccessTime    metrics.Timer
	pointsFailureCounter metrics.Counter
	pointsFailureTime    metrics.Timer
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
		Points:   []influx.Point{},
		Database: DBName,
	}
	for {
		select {
		case point, open := <-p.destination.points:
			if open {
				seriesName := point.SeriesName()
				delivery.Points = append(delivery.Points, influx.Point{Measurement: seriesName})
			} else {
				return delivery, true
			}
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
