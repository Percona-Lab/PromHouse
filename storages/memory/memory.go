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

// Package memory provides functional dummy storage for testing.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/base"
	"github.com/Percona-Lab/PromHouse/utils/timeseries"
)

const (
	namespace = "promhouse"
	subsystem = "memory"
)

// memory is a functional dummy storage for testing.
type memory struct {
	rw      sync.RWMutex
	metrics map[uint64][]*prompb.Label
	samples map[uint64][]*prompb.Sample
	mDummy  prometheus.Counter
}

func New() base.Storage {
	m := &memory{
		metrics: make(map[uint64][]*prompb.Label, 8192),
		samples: make(map[uint64][]*prompb.Sample, 8192),
		mDummy: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dummy",
			Help:      "dummy",
		}),
	}
	return m
}

func (m *memory) Describe(c chan<- *prometheus.Desc) {
	m.mDummy.Describe(c)
}

func (m *memory) Collect(c chan<- prometheus.Metric) {
	m.mDummy.Collect(c)
}

func (m *memory) Read(ctx context.Context, queries []base.Query) (*prompb.ReadResponse, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	res := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(queries)),
	}
	for i, q := range queries {
		res.Results[i] = new(prompb.QueryResult)
		for f, metric := range m.metrics {
			if q.Matchers.MatchLabels(metric) {
				var ts *prompb.TimeSeries
				start, end := int64(q.Start), int64(q.End)
				for _, sp := range m.samples[f] {
					if sp.TimestampMs < start {
						continue
					}
					if sp.TimestampMs > end {
						break
					}
					if ts == nil {
						ts = &prompb.TimeSeries{
							Labels: metric,
						}
					}
					ts.Samples = append(ts.Samples, sp)
				}
				if ts != nil {
					res.Results[i].TimeSeries = append(res.Results[i].TimeSeries, ts)
				}
			}
		}
	}

	return res, nil
}

func (m *memory) Write(ctx context.Context, data *prompb.WriteRequest) error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	for _, ts := range data.TimeSeries {
		timeseries.SortLabels(ts.Labels)
		f := timeseries.Fingerprint(ts.Labels)
		m.metrics[f] = ts.Labels

		s := m.samples[f]
		s = append(s, ts.Samples...)
		less := func(i, j int) bool { return s[i].TimestampMs < s[j].TimestampMs }
		if !sort.SliceIsSorted(s, less) {
			sort.Slice(s, less)
		}
		m.samples[f] = s
	}

	return nil
}

// check interfaces
var (
	_ base.Storage = (*memory)(nil)
)
