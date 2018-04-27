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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	upstream = `# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 6.91e-05
go_gc_duration_seconds{quantile="0.25"} 0.0001714
go_gc_duration_seconds{quantile="0.5"} 0.0002509
go_gc_duration_seconds{quantile="0.75"} 0.0010951
go_gc_duration_seconds{quantile="1"} 0.0053027
go_gc_duration_seconds_sum 0.0285907
go_gc_duration_seconds_count 28
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 38
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.9.2"} 1
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total 1.293258864e+09
# HELP node_netstat_TcpExt_TCPSackMerged Statistic TcpExtTCPSackMerged.
# TYPE node_netstat_TcpExt_TCPSackMerged untyped
node_netstat_TcpExt_TCPSackMerged 0
`

	result = `# HELP go_gc_duration_seconds A summary of the GC invocation durations.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{instance="instance1",quantile="0"} 6.91e-05
go_gc_duration_seconds{instance="instance1",quantile="0.25"} 0.0001714
go_gc_duration_seconds{instance="instance1",quantile="0.5"} 0.0002509
go_gc_duration_seconds{instance="instance1",quantile="0.75"} 0.0010951
go_gc_duration_seconds{instance="instance1",quantile="1"} 0.0053027
go_gc_duration_seconds_sum{instance="instance1"} 0.0285907
go_gc_duration_seconds_count{instance="instance1"} 28
go_gc_duration_seconds{instance="instance2",quantile="0"} 6.91e-05
go_gc_duration_seconds{instance="instance2",quantile="0.25"} 0.0001714
go_gc_duration_seconds{instance="instance2",quantile="0.5"} 0.0002509
go_gc_duration_seconds{instance="instance2",quantile="0.75"} 0.0010951
go_gc_duration_seconds{instance="instance2",quantile="1"} 0.0053027
go_gc_duration_seconds_sum{instance="instance2"} 0.0285907
go_gc_duration_seconds_count{instance="instance2"} 28
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines{instance="instance1"} 38
go_goroutines{instance="instance2"} 41
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{instance="",version="instance1"} 1
go_info{instance="",version="instance2"} 1
# HELP go_memstats_alloc_bytes_total Total number of bytes allocated, even if freed.
# TYPE go_memstats_alloc_bytes_total counter
go_memstats_alloc_bytes_total{instance="instance1"} 1.335822613e+09
go_memstats_alloc_bytes_total{instance="instance2"} 1.277148528e+09
# HELP node_netstat_TcpExt_TCPSackMerged Statistic TcpExtTCPSackMerged.
# TYPE node_netstat_TcpExt_TCPSackMerged untyped
node_netstat_TcpExt_TCPSackMerged{instance="instance1"} 0
node_netstat_TcpExt_TCPSackMerged{instance="instance2"} 0
`
)

func TestFaker(t *testing.T) {
	faker := newFaker("instance%d", 2)
	faker.sort = true
	faker.rnd.Seed(1)

	src := strings.NewReader(upstream)
	var dst bytes.Buffer
	require.NoError(t, faker.generate(&dst, src))
	expected := strings.Split(result, "\n")
	actual := strings.Split(dst.String(), "\n")
	assert.Equal(t, expected, actual, "=== expected:\n%s\n\n=== actual:\n%s\n", result, dst.String())
}
