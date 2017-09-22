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

package storages

import (
	"context"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// Memory is a functional dummy storage for testing.
type Memory struct {
	metrics map[uint64][]*prompb.Label
	samples map[uint64][]*prompb.Sample
	rw      sync.RWMutex
}

func NewMemory() *Memory {
	return &Memory{
		metrics: make(map[uint64][]*prompb.Label, 8192),
		samples: make(map[uint64][]*prompb.Sample, 8192),
	}
}

func (m *Memory) Describe(c chan<- *prometheus.Desc) {
}

func (m *Memory) Collect(c chan<- prometheus.Metric) {
}

func (m *Memory) Read(ctx context.Context, queries []Query) (*prompb.ReadResponse, error) {
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
					if sp.Timestamp < start {
						continue
					}
					if sp.Timestamp > end {
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
					res.Results[i].Timeseries = append(res.Results[i].Timeseries, ts)
				}
			}
		}
	}

	return res, nil
}

func (m *Memory) Write(ctx context.Context, data *prompb.WriteRequest) error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	for _, ts := range data.Timeseries {
		sortLabels(ts.Labels)
		f := fingerprint(ts.Labels)
		m.metrics[f] = ts.Labels

		s := m.samples[f]
		s = append(s, ts.Samples...)
		less := func(i, j int) bool { return s[i].Timestamp < s[j].Timestamp }
		if !sort.SliceIsSorted(s, less) {
			sort.Slice(s, less)
		}
		m.samples[f] = s
	}

	return nil
}

// check interface
var _ Storage = (*Memory)(nil)
