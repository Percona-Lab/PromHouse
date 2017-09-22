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
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// Blackhole is a non-functional dummy storage for testing.
type Blackhole struct{}

func NewBlackhole() *Blackhole {
	return new(Blackhole)
}

func (m *Blackhole) Describe(c chan<- *prometheus.Desc) {
}

func (m *Blackhole) Collect(c chan<- prometheus.Metric) {
}

func (m *Blackhole) Read(ctx context.Context, queries []Query) (*prompb.ReadResponse, error) {
	return nil, nil
}

func (m *Blackhole) Write(ctx context.Context, data *prompb.WriteRequest) error {
	return nil
}

// check interface
var _ Storage = (*Blackhole)(nil)
