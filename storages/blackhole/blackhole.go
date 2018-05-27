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

const (
	namespace = "promhouse"
	subsystem = "blackhole"
)

// blackhole is a non-functional dummy storage for testing.
type blackhole struct {
	mDummy prometheus.Counter
}

func New() base.Storage {
	b := &blackhole{
		mDummy: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dummy",
			Help:      "dummy",
		}),
	}
	return b
}

func (b *blackhole) Describe(c chan<- *prometheus.Desc) {
	b.mDummy.Describe(c)
}

func (b *blackhole) Collect(c chan<- prometheus.Metric) {
	b.mDummy.Collect(c)
}

func (b *blackhole) Read(ctx context.Context, queries []base.Query) (*prompb.ReadResponse, error) {
	res := &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(queries)),
	}
	for i := range res.Results {
		res.Results[i] = &prompb.QueryResult{}
	}
	return res, nil
}

func (b *blackhole) Write(ctx context.Context, data *prompb.WriteRequest) error {
	return nil
}

// check interfaces
var (
	_ base.Storage = (*blackhole)(nil)
)
