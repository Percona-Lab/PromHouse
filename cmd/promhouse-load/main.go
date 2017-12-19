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
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/common/model"
)

func main() {
	log.SetFlags(0)

	var (
		prometheusF = flag.String("prometheus", "http://127.0.0.1:9090/", "Prometheus endpoint")
		lastF       = model.Duration(30 * 24 * time.Hour)
		stepF       = flag.Duration("step", time.Hour, "step")
	)
	flag.Var(&lastF, "last", "Starting point")
	flag.Parse()

	pc := &prometheusClient{
		base:   *prometheusF,
		client: new(http.Client),
	}

	end := time.Now().Truncate(time.Second).UTC()
	begin := end.Add(-time.Duration(lastF))
	timeSeries, err := pc.requestTimeSeries(begin, end)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Got %d time series from %s to %s.", len(timeSeries), begin, end)

	f, err := os.Create("1.bin")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fc := &fileClient{
		f: f,
	}

	for _, ts := range timeSeries {
		log.Print(ts)
		for start := begin; start.Before(end); start = start.Add(*stepF) {
			m, err := pc.requestMetrics(start, start.Add(*stepF), ts)
			if err != nil {
				log.Fatal(err)
			}

			if err = fc.write(m); err != nil {
				log.Fatal(err)
			}
		}
	}
}
