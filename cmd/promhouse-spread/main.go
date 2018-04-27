// PromHouse
// Copyright (C) 2017 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/utils/duration"
)

type client struct {
	upstream string
	storage  string
	http     *http.Client
}

func newClient(upstream, storage string) *client {
	return &client{
		upstream: upstream,
		storage:  storage,
		http:     http.DefaultClient,
		// http: &http.Client{
		// 	Transport: &http.Transport{
		// 		MaxIdleConnsPerHost: conc,
		// 	},
		// 	Timeout: time.Second,
		// },
	}
}

func (c *client) get() (model.Vector, error) {
	req, err := http.NewRequest("GET", c.upstream, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, b)
	}

	d := expfmt.NewDecoder(resp.Body, expfmt.ResponseFormat(resp.Header))
	mfs := make([]*dto.MetricFamily, 0, 100)
	for {
		mf := new(dto.MetricFamily)
		if err = d.Decode(mf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		mfs = append(mfs, mf)
	}

	v, err := expfmt.ExtractSamples(&expfmt.DecodeOptions{}, mfs...)
	if err != nil {
		return nil, err
	}
	sort.Sort(v)
	return v, nil
}

func (c *client) write(request *prompb.WriteRequest) error {
	b, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	b = snappy.Encode(nil, b)
	req, err := http.NewRequest("POST", c.storage, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("%d: %s", resp.StatusCode, b)
	}
	return nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		storageF                = kingpin.Flag("remote", "Remote storage write endpoint").Default("http://127.0.0.1:7781/write").String()
		writersF                = kingpin.Flag("writers", "Number of concurrent remote storage writers").Default("4").Int()
		upstreamF               = kingpin.Flag("upstream", "Upstream metrics endpoint").Default("http://127.0.0.1:9116/metrics").String()
		upstreamJobF            = kingpin.Flag("upstream-job", "Upstream job name").Default("promhouse-spread").String()
		upstreamScrapeIntervalF = duration.FromFlag(kingpin.Flag("upstream-scrape-interval", "How often scrape upstream for metrics").Default("1s"))
		instancesF              = kingpin.Flag("instances", "Number of instances to generate").Default("100").Int()
		spreadF                 = duration.FromFlag(kingpin.Flag("spread", "Spread metrics over that interval").Default("90d"))
		intervalF               = duration.FromFlag(kingpin.Flag("interval", "Spread metrics step").Default("1s"))
		debugF                  = kingpin.Flag("debug", "Enable debug output").Bool()
	)
	kingpin.Parse()

	logrus.SetLevel(logrus.InfoLevel)
	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var sharedVector model.Vector
	var rw sync.RWMutex
	c := newClient(*upstreamF, *storageF)

	// run upstream scraper
	go func() {
		for range time.Tick(time.Duration(*upstreamScrapeIntervalF)) {
			v, err := c.get()
			if err != nil {
				log.Fatal(err)
			}
			rw.Lock()
			sharedVector = v
			rw.Unlock()
		}
	}()

	// run writers
	requests := make(chan *prompb.WriteRequest)
	var wg sync.WaitGroup
	for i := 0; i < *writersF; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			nextReport := time.Now()

			for req := range requests {
				start := time.Now()
				if err := c.write(req); err != nil {
					log.Fatal(err)
				}

				if time.Now().After(nextReport) {
					log.Printf("writer %d: %d time series in %s", id, len(req.TimeSeries), time.Since(start))
					nextReport = time.Now().Add(5 * time.Second)
				}
			}
		}(i)
	}

	stop := time.Now().Truncate(time.Minute).UTC()
	start := stop.Add(-time.Duration(*spreadF))
	steps := int(stop.Sub(start) / time.Duration(*intervalF))
	timestamp := start
	instanceFormat := fmt.Sprintf("instance-%%0%dd", len(strconv.Itoa(*instancesF-1)))
	nextReport := time.Now()
	for step := 0; step < steps; step++ {
		rw.RLock()
		v := sharedVector
		rw.RUnlock()
		if len(v) == 0 {
			time.Sleep(time.Duration(*upstreamScrapeIntervalF))
			continue
		}

		var samples int
		req := &prompb.WriteRequest{
			TimeSeries: make([]*prompb.TimeSeries, 0, len(v)**instancesF),
		}
		for _, s := range v {
			ts := &prompb.TimeSeries{
				Labels: make([]*prompb.Label, 1, len(s.Metric)+1),
			}
			for k, v := range s.Metric {
				ts.Labels = append(ts.Labels, &prompb.Label{
					Name:  string(k),
					Value: string(v),
				})
			}
			ts.Labels = append(ts.Labels, &prompb.Label{
				Name:  "job",
				Value: *upstreamJobF,
			})

			ts.Samples = []*prompb.Sample{
				{
					Value:       float64(s.Value),
					TimestampMs: int64(model.TimeFromUnixNano(timestamp.UnixNano())),
				},
			}
			samples++

			// make time series for each instance
			for i := 0; i < *instancesF; i++ {
				ts.Labels[0] = &prompb.Label{
					Name:  "instance",
					Value: fmt.Sprintf(instanceFormat, i),
				}
				req.TimeSeries = append(req.TimeSeries, ts)
			}
		}

		requests <- req

		if time.Now().After(nextReport) {
			samples *= *instancesF
			log.Printf("%d/%d %.1f%% %s: %d timeseries, %d samples",
				step, steps, float64(step)/float64(steps)*100, timestamp, len(req.TimeSeries), samples)
			nextReport = time.Now().Add(5 * time.Second)
		}

		timestamp = timestamp.Add(time.Duration(*intervalF))
	}

	close(requests)
	wg.Wait()
}
