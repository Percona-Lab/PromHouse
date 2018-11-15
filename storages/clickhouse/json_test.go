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

package clickhouse

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/test"
	"github.com/Percona-Lab/PromHouse/utils/timeseries"
)

func TestMarshalMetricsAndLabels(t *testing.T) {
	for _, labels := range [][]*prompb.Label{
		{},
		{
			{Name: "", Value: ""},
		}, {
			{Name: "label", Value: ""},
		}, {
			{Name: "", Value: "value"},
		}, {
			{Name: "__name__", Value: "normal"},
			{Name: "instance", Value: "foo"},
			{Name: "job", Value: "bar"},
		}, {
			{Name: "__name__", Value: "funny_1"},
			{Name: "label", Value: ""},
		}, {
			{Name: "__name__", Value: "funny_2"},
			{Name: "label", Value: "'`\"\\"},
		}, {
			{Name: "__name__", Value: "funny_3"},
			{Name: "label", Value: "''``\"\"\\\\"},
		}, {
			{Name: "__name__", Value: "funny_4"},
			{Name: "label", Value: "'''```\"\"\"\\\\\\"},
		}, {
			{Name: "__name__", Value: "funny_5"},
			{Name: "label", Value: `\ \\ \\\\ \\\\`},
		}, {
			{Name: "__name__", Value: "funny_6"},
			{Name: "label", Value: "🆗"},
		},
	} {
		b1 := marshalLabels(labels, nil)
		b2, err := json.Marshal(test.MakeMetric(labels))
		require.NoError(t, err)

		m1 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b1, &m1))
		m2 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b2, &m2))
		assert.Equal(t, m2, m1)

		l1, err := unmarshalLabels(b1)
		require.NoError(t, err)
		l2, err := unmarshalLabels(b2)
		require.NoError(t, err)
		timeseries.SortLabels(l1)
		timeseries.SortLabels(l2)
		assert.Equal(t, labels, l1)
		assert.Equal(t, labels, l2)
	}
}

var (
	sink []byte

	labelsB = []*prompb.Label{
		{Name: "__name__", Value: "http_requests_total"},
		{Name: "code", Value: "200"},
		{Name: "handler", Value: "query"},
	}
)

func BenchmarkMarshalJSON(b *testing.B) {
	var err error
	metric := test.MakeMetric(labelsB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink, err = json.Marshal(metric)
	}
	b.StopTimer()

	require.NoError(b, err)
}

func BenchmarkMarshalLabels(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink = marshalLabels(labelsB, sink[:0])
	}
}
