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
