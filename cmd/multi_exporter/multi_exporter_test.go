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
`

	dup = `# HELP go_gc_duration_seconds A summary of the GC invocation durations.
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
go_goroutines{instance="instance2"} 38
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.9.2",instance="instance1"} 1
go_info{version="go1.9.2",instance="instance2"} 1
`
)

func TestMulti(t *testing.T) {
	src := strings.NewReader(upstream)
	var dst bytes.Buffer
	require.NoError(t, multi(&dst, src, "instance%d", 2))
	assert.Equal(t, strings.Split(dup, "\n"), strings.Split(dst.String(), "\n"))
}
