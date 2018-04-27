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

// Package blackhole provides non-functional dummy storage for testing.
package blackhole

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/base"
)

// blackhole is a non-functional dummy storage for testing.
type blackhole struct{}

func New() base.Storage {
	return new(blackhole)
}

func (m *blackhole) Describe(c chan<- *prometheus.Desc) {
}

func (m *blackhole) Collect(c chan<- prometheus.Metric) {
}

func (m *blackhole) Read(ctx context.Context, queries []base.Query) (*prompb.ReadResponse, error) {
	return nil, nil
}

func (m *blackhole) Write(ctx context.Context, data *prompb.WriteRequest) error {
	return nil
}

// check interfaces
var (
	_ base.Storage = (*blackhole)(nil)
)
