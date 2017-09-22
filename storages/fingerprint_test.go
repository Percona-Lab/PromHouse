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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Percona-Lab/PromHouse/prompb"
)

func TestFingerprints(t *testing.T) {
	// special case - zero labels
	expected := uint64(makeMetric(nil).Fingerprint())
	actual := fingerprint(nil)
	assert.Equal(t, expected, actual)

	for _, ts := range getData().TimeSeries {
		expected = uint64(makeMetric(ts.Labels).Fingerprint())
		actual = fingerprint(ts.Labels)
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
		actualB = uint64(makeMetric(labelsB).Fingerprint())
	}
	b.StopTimer()
	assert.Equal(b, expectedB, actualB)
}

func BenchmarkCopied(b *testing.B) {
	for i := 0; i < b.N; i++ {
		actualB = fingerprint(labelsB)
	}
	b.StopTimer()
	assert.Equal(b, expectedB, actualB)
}
