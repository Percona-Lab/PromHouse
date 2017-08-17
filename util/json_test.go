package util

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalLabels(t *testing.T) {
	for _, labels := range []map[string]string{
		{"__name__": "normal", "instance": "foo", "job": "bar"},
		{"__name__": "funny_1", "label": ""},
		{"__name__": "funny_2", "label": "'`\"\\"},
		{"__name__": "funny_3", "label": "''``\"\"\\\\"},
		{"__name__": "funny_4", "label": "'''```\"\"\"\\\\\\"},
		{"__name__": "funny_5", "label": `\ \\ \\\\ \\\\`},
		{"__name__": "funny_6", "label": "🆗"},
	} {
		expectedB, err := json.Marshal(labels)
		require.NoError(t, err)
		actualB := MarshalLabels(labels)
		actual := make(map[string]string)
		err = json.Unmarshal(actualB, &actual)
		assert.NoError(t, err)
		require.Equal(t, labels, actual, "\nexpected:\n\t%s\nactual:\n\t%s", expectedB, actualB)
	}
}

var labels = map[string]string{"__name__": "normal", "instance": "foo", "job": "bar"}
var sink []byte

func BenchmarkMarshalJSON(b *testing.B) {
	var err error

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink, err = json.Marshal(labels)
	}
	b.StopTimer()

	require.NoError(b, err)
}

func BenchmarkMarshalLabels(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = MarshalLabels(labels)
	}
	b.StopTimer()
}
