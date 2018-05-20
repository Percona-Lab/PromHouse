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

package main

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/prompb"
)

var (
	metrics = []byte(`
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 38
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.9.2"} 1
`)
)

func TestDecodeMetrics(t *testing.T) {
	rc := ioutil.NopCloser(bytes.NewReader(metrics))
	now := time.Date(2018, 5, 20, 16, 33, 23, 123456789, time.UTC)
	client := &exporterClient{sort: true}
	ts, err := client.decodeMetrics(rc, nil, now)
	require.NoError(t, err)
	expected := []*prompb.TimeSeries{
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "go_goroutines"},
			},
			Samples: []*prompb.Sample{
				{Value: 38, TimestampMs: 1526834003123},
			},
		},
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "go_info"},
				{Name: "version", Value: "go1.9.2"},
			},
			Samples: []*prompb.Sample{
				{Value: 1, TimestampMs: 1526834003123},
			},
		},
	}
	assert.Equal(t, expected, ts)
}
