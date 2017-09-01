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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalMetric(t *testing.T) {
	for _, metric := range []model.Metric{
		{"__name__": "normal", "instance": "foo", "job": "bar"},
		{"__name__": "funny_1", "label": ""},
		{"__name__": "funny_2", "label": "'`\"\\"},
		{"__name__": "funny_3", "label": "''``\"\"\\\\"},
		{"__name__": "funny_4", "label": "'''```\"\"\"\\\\\\"},
		{"__name__": "funny_5", "label": `\ \\ \\\\ \\\\`},
		{"__name__": "funny_6", "label": "🆗"},
	} {
		expectedB, err := json.Marshal(metric)
		require.NoError(t, err)
		actualB := marshalMetric(metric)
		actual := make(model.Metric)
		err = json.Unmarshal(actualB, &actual)
		assert.NoError(t, err)
		require.Equal(t, metric, actual, "\nexpected:\n\t%s\nactual:\n\t%s", expectedB, actualB)
	}
}

var (
	metric = model.Metric{"__name__": "normal", "instance": "foo", "job": "bar"}
	sink   []byte
)

func BenchmarkMarshalJSON(b *testing.B) {
	var err error

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink, err = json.Marshal(metric)
	}
	b.StopTimer()

	require.NoError(b, err)
}

func BenchmarkMarshalMetric(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = marshalMetric(metric)
	}
	b.StopTimer()
}
