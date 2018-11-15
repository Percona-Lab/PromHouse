// Copyright 2017, 2018 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "fake_exporter"

type exporter struct {
	scrapesTotal prometheus.Counter
}

func newExporter() *exporter {
	return &exporter{
		scrapesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "scrapes_total",
			Help:      "Total number of times fake_exporter was scraped for metrics.",
		}),
	}
}

func (e *exporter) Describe(ch chan<- *prometheus.Desc) {
	e.scrapesTotal.Describe(ch)
}

func (e *exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrapesTotal.Collect(ch)
}

// check interfaces
var (
	_ prometheus.Collector = (*exporter)(nil)
)
