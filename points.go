package main

import "time"

// Encodes eries Type information
type seriesType int

const (
	routerRequest seriesType = iota
	routerEvent
	dynoMem
	dynoLoad
	dynoEvents
	numSeries
)

var (
	tagColumns = [][]string{
		[]string{"status"},                                      // Router
		[]string{"code"},                                        // EventsRouter
		[]string{"source", "dynoType"},                          // DynoMem
		[]string{"source", "dynoType"},                          // DynoLoad
		[]string{"what", "type", "code", "message", "dynoType"}, // DynoEvents
	}

	fieldColumns = [][]string{
		[]string{"service"}, // Router
		[]string{"count"},   // EventsRouter
		[]string{"memory_cache", "memory_pgpgin", "memory_pgpgout", "memory_rss", "memory_swap", "memory_total"}, // DynoMem
		[]string{"load_avg_1m", "load_avg_5m", "load_avg_15m"},                                                   // DynoLoad
		[]string{"count"},                                                                                        // DynoEvents
	}

	seriesNames = []string{"router", "events.router", "dyno.mem", "dyno.load", "events.dyno"}
)

func (st seriesType) Name() string {
	return seriesNames[st]
}

func (st seriesType) TagColumns() []string {
	return tagColumns[st]
}

func (st seriesType) FieldColumns() []string {
	return fieldColumns[st]
}

// Holds data around a data point
type point struct {
	Token  string
	Type   seriesType
	Tags   []string
	Fields []interface{}
	Time   time.Time
}

func (p point) SeriesName() string {
	return p.Type.Name() + "." + p.Token
}
