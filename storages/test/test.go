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

// Package test provides common storage testing code.
package test

import (
	"time"

	"github.com/prometheus/common/model"

	"github.com/Percona-Lab/PromHouse/prompb"
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
