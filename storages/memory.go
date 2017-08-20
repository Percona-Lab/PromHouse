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
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// Memory is a functional dummy storage for testing.
type Memory struct {
	metrics []model.Metric
	samples map[model.Fingerprint][]*prompb.Sample
	rw      sync.RWMutex
}

func NewMemory() *Memory {
	return &Memory{
		metrics: make([]model.Metric, 0, 1000),
		samples: make(map[model.Fingerprint][]*prompb.Sample),
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
		for _, metric := range m.metrics {
			if q.Matchers.Match(metric) {
				var ts *prompb.TimeSeries
				samples := m.samples[metric.Fingerprint()]
				start, end := int64(q.Start), int64(q.End)
				for _, sp := range samples {
					if sp.Timestamp < start {
						continue
					}
					if sp.Timestamp > end {
						break
					}
					if ts == nil {
						// convert model.Metric to []*prompb.Label
						labels := make([]*prompb.Label, 0, len(metric))
						for n, v := range metric {
							labels = append(labels, &prompb.Label{
								Name:  string(n),
								Value: string(v),
							})
						}

						ts = &prompb.TimeSeries{
							Labels: labels,
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
		// convert []*prompb.Label to model.Metric
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		var found bool
		for _, m := range m.metrics {
			if m.Equal(metric) {
				found = true
				break
			}
		}
		if !found {
			m.metrics = append(m.metrics, metric)
		}

		f := metric.Fingerprint()
		v := m.samples[f]
		v = append(v, ts.Samples...)
		less := func(i, j int) bool { return v[i].Timestamp < v[j].Timestamp }
		if !sort.SliceIsSorted(v, less) {
			sort.Slice(v, less)
		}
		m.samples[f] = v
	}

	return nil
}

// check interface
var _ Storage = (*Memory)(nil)
