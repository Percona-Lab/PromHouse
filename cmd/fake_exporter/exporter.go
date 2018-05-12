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
