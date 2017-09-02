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
	"encoding/json"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalMetricsAndLabels(t *testing.T) {
	for _, labels := range [][]*prompb.Label{
		{
			&prompb.Label{Name: "__name__", Value: "normal"},
			&prompb.Label{Name: "instance", Value: "foo"},
			&prompb.Label{Name: "job", Value: "bar"},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_1"},
			&prompb.Label{Name: "label", Value: ""},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_2"},
			&prompb.Label{Name: "label", Value: "'`\"\\"},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_3"},
			&prompb.Label{Name: "label", Value: "''``\"\"\\\\"},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_4"},
			&prompb.Label{Name: "label", Value: "'''```\"\"\"\\\\\\"},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_5"},
			&prompb.Label{Name: "label", Value: `\ \\ \\\\ \\\\`},
		}, {
			&prompb.Label{Name: "__name__", Value: "funny_6"},
			&prompb.Label{Name: "label", Value: "🆗"},
		},
	} {
		metric := makeMetric(labels)
		b1 := marshalMetric(metric)
		b2 := marshalLabels(labels)
		b3, err := json.Marshal(metric)
		require.NoError(t, err)

		m1 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b1, &m1))
		m2 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b2, &m2))
		m3 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b3, &m3))
		assert.Equal(t, m3, m1)
		assert.Equal(t, m3, m2)

		l1, err := unmarshalLabels(b1)
		require.NoError(t, err)
		l2, err := unmarshalLabels(b2)
		require.NoError(t, err)
		l3, err := unmarshalLabels(b3)
		require.NoError(t, err)
		assert.Equal(t, labels, l1)
		assert.Equal(t, labels, l2)
		assert.Equal(t, labels, l3)
	}
}

var sink []byte

func BenchmarkMarshalJSON(b *testing.B) {
	var err error
	metric := makeMetric(labelsB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink, err = json.Marshal(metric)
	}
	b.StopTimer()

	require.NoError(b, err)
}

func BenchmarkMarshalMetric(b *testing.B) {
	metric := makeMetric(labelsB)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = marshalMetric(metric)
	}
}

func BenchmarkMarshalLabels(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink = marshalLabels(labelsB)
	}
}
