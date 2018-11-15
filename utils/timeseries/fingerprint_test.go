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

package timeseries

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/test"
)

func TestFingerprints(t *testing.T) {
	// special case - zero labels
	expected := uint64(test.MakeMetric(nil).Fingerprint())
	actual := Fingerprint(nil)
	assert.Equal(t, expected, actual)

	for _, ts := range test.GetData().TimeSeries {
		expected = uint64(test.MakeMetric(ts.Labels).Fingerprint())
		actual = Fingerprint(ts.Labels)
		assert.Equal(t, expected, actual)
	}
}

var (
	labelsB = []*prompb.Label{
		{Name: "__name__", Value: "http_requests_total"},
		{Name: "code", Value: "200"},
		{Name: "handler", Value: "query"},
	}
	expectedB = uint64(0x145426e4f81508d1)
	actualB   uint64
)

func BenchmarkOriginal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		actualB = uint64(test.MakeMetric(labelsB).Fingerprint())
	}
	b.StopTimer()
	assert.Equal(b, expectedB, actualB)
}

func BenchmarkCopied(b *testing.B) {
	for i := 0; i < b.N; i++ {
		actualB = Fingerprint(labelsB)
	}
	b.StopTimer()
	assert.Equal(b, expectedB, actualB)
}
