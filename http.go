package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	auth "github.com/heroku/authenticater"
	influx "github.com/influxdb/influxdb/client"
)

var influxDbStaleTimeout = 24 * time.Minute // Would be nice to make this smaller, but it lags due to continuous queries.
var influxDbSeriesCheckQueries = []string{
	"select * from dyno.load.%s limit 1",
	"select * from dyno.mem.%s limit 1",
}

var healthCheckClientsLock = new(sync.Mutex)
var healthCheckClients = make(map[string]*influx.Client)

type server struct {
	sync.WaitGroup
	connectionCloser chan struct{}
	hashRing         *hashRing
	http             *http.Server
	shutdownChan     shutdownChan
	isShuttingDown   bool
	credStore        map[string]string

	// scheduler based sampling lock for writing to recentTokens
	tokenLock        *int32
	recentTokensLock *sync.RWMutex
	recentTokens     map[url.URL]string
}

func newServer(httpServer *http.Server, ath auth.Authenticater, hashRing *hashRing) *server {
	s := &server{
		connectionCloser: make(chan struct{}),
		shutdownChan:     make(chan struct{}),
		http:             httpServer,
		hashRing:         hashRing,
		credStore:        make(map[string]string),
		tokenLock:        new(int32),
		recentTokensLock: new(sync.RWMutex),
		recentTokens:     make(map[url.URL]string),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/drain", auth.WrapAuth(ath,
		func(w http.ResponseWriter, r *http.Request) {
			s.serveDrain(w, r)
			s.recycleConnection(w)
		}))

	mux.HandleFunc("/health", s.serveHealth)
	mux.HandleFunc("/health/influxdb", auth.WrapAuth(ath, s.serveInfluxDBHealth))
	mux.HandleFunc("/target/", auth.WrapAuth(ath, s.serveTarget))

	s.http.Handler = mux

	return s
}

func (s *server) Close() error {
	s.shutdownChan <- struct{}{}
	return nil
}

func (s *server) scheduleConnectionRecycling(after time.Duration) {
	for !s.isShuttingDown {
		time.Sleep(after)
		s.connectionCloser <- struct{}{}
	}
}

func (s *server) recycleConnection(w http.ResponseWriter) {
	select {
	case <-s.connectionCloser:
		w.Header().Set("Connection", "close")
	default:
		if s.isShuttingDown {
			w.Header().Set("Connection", "close")
		}
	}
}

func (s *server) Run(connRecycle time.Duration) {
	go s.awaitShutdown()
	go s.scheduleConnectionRecycling(connRecycle)

	if err := s.http.ListenAndServe(); err != nil {
		log.Fatalln("Unable to start HTTP server: ", err)
	}
}

// Serves a 200 OK, unless shutdown has been requested.
// Shutting down serves a 503 since that's how ELBs implement connection draining.
func (s *server) serveHealth(w http.ResponseWriter, r *http.Request) {
	if s.isShuttingDown {
		http.Error(w, "Shutting Down", 503)
	}

	w.WriteHeader(http.StatusOK)
}

func getHealthCheckClient(u *url.URL, f clientFunc) (*influx.Client, error) {
	healthCheckClientsLock.Lock()
	defer healthCheckClientsLock.Unlock()

	client, exists := healthCheckClients[u.Host]
	if !exists {
		var err error
		clientConfig := createInfluxDBClient(u, f)
		client, err = influx.NewClient(clientConfig)
		if err != nil {
			log.Printf("err=%q at=getHealthCheckClient host=%q", err, u.Host)
			return nil, err
		}

		healthCheckClients[u.Host] = client
	}

	return client, nil
}

func checkRecentToken(client *influx.Client, token, host string, errors chan error) {
	for _, qfmt := range influxDbSeriesCheckQueries {
		query := influx.Query{
			Command:  fmt.Sprintf(qfmt, token),
			Database: "",
		}
		response, err := client.Query(query)
		if err != nil || response.Error() != nil {
			errors <- fmt.Errorf("at=influxdb-health err=%q influx-err=%s host=%q query=%q", err, response.Error(), host, query)
			continue
		}

		row := response.Results[0].Series[0]
		t, ok := row.Values[0][0].(float64)
		if !ok {
			errors <- fmt.Errorf("at=influxdb-health err=\"time column was not a number\" host=%q query=%q", host, query)
			continue
		}

		ts := time.Unix(int64(t), int64(0)).UTC()
		now := time.Now().UTC()
		if now.Sub(ts) > influxDbStaleTimeout {
			errors <- fmt.Errorf("at=influxdb-health err=\"stale data\" host=%q ts=%q now=%q query=%q", host, ts, now, query)
		}
	}
}

func (s *server) checkRecentTokens() []error {
	var errSlice []error

	wg := new(sync.WaitGroup)

	s.recentTokensLock.RLock()
	tokenMap := make(map[url.URL]string)
	for u, token := range s.recentTokens {
		tokenMap[u] = token
	}
	s.recentTokensLock.RUnlock()

	errors := make(chan error, len(tokenMap)*len(influxDbSeriesCheckQueries))

	for u, token := range tokenMap {
		wg.Add(1)
		go func(token string, u url.URL) {
			client, err := getHealthCheckClient(&u, newClientFunc)
			if err != nil {
				return
			}
			checkRecentToken(client, token, u.Host, errors)
			wg.Done()
		}(token, u)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		errSlice = append(errSlice, err)
	}

	return errSlice
}

func (s *server) serveInfluxDBHealth(w http.ResponseWriter, r *http.Request) {
	errors := s.checkRecentTokens()

	if len(errors) > 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		for _, err := range errors {
			w.Write([]byte(err.Error() + "\n"))
			log.Println(err)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *server) awaitShutdown() {
	<-s.shutdownChan
	log.Printf("Shutting down.")
	s.isShuttingDown = true
}
