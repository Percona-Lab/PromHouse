package util

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
		actualB := MarshalMetric(metric)
		actual := make(model.Metric)
		err = json.Unmarshal(actualB, &actual)
		assert.NoError(t, err)
		require.Equal(t, metric, actual, "\nexpected:\n\t%s\nactual:\n\t%s", expectedB, actualB)
	}
}

var metric = model.Metric{"__name__": "normal", "instance": "foo", "job": "bar"}
var sink []byte

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
		sink = MarshalMetric(metric)
	}
	b.StopTimer()
}
