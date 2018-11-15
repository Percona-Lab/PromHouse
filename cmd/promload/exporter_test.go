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

package main

import (
	"bytes"
	"io/ioutil"
	"testing"

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
	client := &exporterClient{sort: true}
	ts, err := client.decodeMetrics(rc, nil)
	require.NoError(t, err)
	expected := []*prompb.TimeSeries{
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "go_goroutines"},
			},
			Samples: []*prompb.Sample{
				{Value: 38},
			},
		},
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "go_info"},
				{Name: "version", Value: "go1.9.2"},
			},
			Samples: []*prompb.Sample{
				{Value: 1},
			},
		},
	}
	assert.Equal(t, expected, ts)
}
