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

// Package test provides common storage testing code.
package test

import (
	"time"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/prometheus/common/model"
)

func GetData() *prompb.WriteRequest {
	start := model.Now().Add(-6 * time.Second)

	return &prompb.WriteRequest{
		TimeSeries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "query"},
				},
				Samples: []*prompb.Sample{
					{Value: 13, TimestampMs: int64(start)},
					{Value: 14, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 14, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 14, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 15, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "400"},
					{Name: "handler", Value: "query_range"},
				},
				Samples: []*prompb.Sample{
					{Value: 9, TimestampMs: int64(start)},
					{Value: 9, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 9, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 11, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 11, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "prometheus"},
				},
				Samples: []*prompb.Sample{
					{Value: 591, TimestampMs: int64(start)},
					{Value: 592, TimestampMs: int64(start.Add(1 * time.Second))},
					{Value: 593, TimestampMs: int64(start.Add(2 * time.Second))},
					{Value: 594, TimestampMs: int64(start.Add(3 * time.Second))},
					{Value: 595, TimestampMs: int64(start.Add(4 * time.Second))},
				},
			},
		},
	}
}

func MakeMetric(labels []*prompb.Label) model.Metric {
	metric := make(model.Metric, len(labels))
	for _, l := range labels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}
