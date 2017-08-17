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
)

type Memory struct {
	metrics []model.Metric
	samples map[model.Fingerprint][]model.SamplePair
	rw      sync.RWMutex
}

func NewMemory() *Memory {
	return &Memory{
		metrics: make([]model.Metric, 0, 1000),
		samples: make(map[model.Fingerprint][]model.SamplePair),
	}
}

func (m *Memory) Describe(c chan<- *prometheus.Desc) {
}

func (m *Memory) Collect(c chan<- prometheus.Metric) {
}

func (m *Memory) Read(ctx context.Context, queries []Query) ([]model.Matrix, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	res := make([]model.Matrix, len(queries))
	for i, q := range queries {
		for _, metric := range m.metrics {
			matches := true
			for _, matcher := range q.Matchers {
				if !matcher.Match(metric) {
					matches = false
					break
				}
			}

			if matches {
				var ss *model.SampleStream
				samples := m.samples[metric.Fingerprint()]
				for _, sp := range samples {
					if sp.Timestamp.Before(q.Start) {
						continue
					}
					if sp.Timestamp.After(q.End) {
						break
					}
					if ss == nil {
						ss = &model.SampleStream{
							Metric: metric,
						}
					}
					ss.Values = append(ss.Values, sp)
				}
				if ss != nil {
					res[i] = append(res[i], ss)
				}
			}
		}
	}

	return res, nil
}

func (m *Memory) Write(ctx context.Context, data model.Matrix) error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	for _, ss := range data {
		var found bool
		for _, m := range m.metrics {
			if m.Equal(ss.Metric) {
				found = true
				break
			}
		}
		if !found {
			m.metrics = append(m.metrics, ss.Metric)
		}

		f := ss.Metric.Fingerprint()
		v := append(m.samples[f], ss.Values...)
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
