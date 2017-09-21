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
	"sort"
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
		b1 := marshalLabels(labels, nil)
		b2, err := json.Marshal(metric)
		require.NoError(t, err)

		m1 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b1, &m1))
		m2 := make(model.Metric)
		require.NoError(t, json.Unmarshal(b2, &m2))
		assert.Equal(t, m2, m1)

		byName := func(labels []*prompb.Label) func(i, j int) bool {
			return func(i, j int) bool {
				return labels[i].Name < labels[j].Name
			}
		}

		l1, err := unmarshalLabels(b1)
		require.NoError(t, err)
		l2, err := unmarshalLabels(b2)
		require.NoError(t, err)
		sort.Slice(l2, byName(l2)) // b2 is created by json.Marshal, so we need to restore order
		assert.Equal(t, labels, l1)
		assert.Equal(t, labels, l2)
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

func BenchmarkMarshalLabels(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink = marshalLabels(labelsB, sink[:0])
	}
}
