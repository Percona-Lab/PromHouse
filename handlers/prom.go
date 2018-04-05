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

// Package handlers provides Prometheus Remote API handlers.
package handlers

import (
	"context"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/base"
)

const (
	namespace = "promhouse"
	subsystem = "api"
)

type PromAPI struct {
	Storage base.Storage
	Logger  *logrus.Entry

	mReadsStarted, mWritesStarted prometheus.Counter
	mReads, mWrites               *prometheus.SummaryVec
}

func NewPromAPI(storage base.Storage, logger *logrus.Entry) *PromAPI {
	return &PromAPI{
		Storage: storage,
		Logger:  logger,

		mReadsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "reads_started",
			Help:      "Number of started reads.",
		}),
		mWritesStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "writes_started",
			Help:      "Number of started writes.",
		}),
		mReads: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "reads",
			Help:      "Durations of reads by result type: ok, canceled, other.",
		}, []string{"type"}),
		mWrites: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "writes",
			Help:      "Durations of writes by result type: ok, canceled, other.",
		}, []string{"type"}),
	}
}

func (p *PromAPI) Describe(c chan<- *prometheus.Desc) {
	p.mReadsStarted.Describe(c)
	p.mWritesStarted.Describe(c)
	p.mReads.Describe(c)
	p.mWrites.Describe(c)
}

func (p *PromAPI) Collect(c chan<- prometheus.Metric) {
	p.mReadsStarted.Collect(c)
	p.mWritesStarted.Collect(c)
	p.mReads.Collect(c)
	p.mWrites.Collect(c)
}

// Store pointers, not slices. See https://staticcheck.io/docs/staticcheck#SA6002
var snappyPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024)
		return &b
	},
}

func readPB(req *http.Request, pb proto.Message) error {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}

	dst := *snappyPool.Get().(*[]byte)
	dst = dst[:cap(dst)]
	b, err := snappy.Decode(dst, compressed)
	if err == nil {
		err = proto.Unmarshal(b, pb)
	}
	snappyPool.Put(&b)
	return err
}

func (p *PromAPI) convertReadRequest(request *prompb.ReadRequest) []base.Query {
	queries := make([]base.Query, len(request.Queries))
	for i, rq := range request.Queries {
		q := base.Query{
			Start:    model.Time(rq.StartTimestampMs),
			End:      model.Time(rq.EndTimestampMs),
			Matchers: make([]base.Matcher, len(rq.Matchers)),
		}

		for j, m := range rq.Matchers {
			var t base.MatchType
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				t = base.MatchEqual
			case prompb.LabelMatcher_NEQ:
				t = base.MatchNotEqual
			case prompb.LabelMatcher_RE:
				t = base.MatchRegexp
			case prompb.LabelMatcher_NRE:
				t = base.MatchNotRegexp
			default:
				p.Logger.Panicf("expectation failed: unexpected matcher %d", m.Type)
			}

			q.Matchers[j] = base.Matcher{
				Type:  t,
				Name:  m.Name,
				Value: m.Value,
			}
		}

		queries[i] = q
	}

	return queries
}

func errResponseType(err error) string {
	switch err {
	case nil:
		return "ok"
	case context.Canceled, context.DeadlineExceeded:
		return "canceled"
	default:
		return "other"
	}
}

func (p *PromAPI) Read(ctx context.Context, rw http.ResponseWriter, req *http.Request) (err error) {
	// track time and response status
	p.mReadsStarted.Inc()
	start := time.Now()
	defer func() {
		p.mReads.WithLabelValues(errResponseType(err)).Observe(time.Since(start).Seconds())
	}()

	var request prompb.ReadRequest
	if err = readPB(req, &request); err != nil {
		return
	}

	// read from storage
	queries := p.convertReadRequest(&request)
	for i, q := range queries {
		p.Logger.Infof("Query %d: %s", i+1, q)
	}
	var response *prompb.ReadResponse
	if response, err = p.Storage.Read(ctx, queries); err != nil {
		return err
	}
	p.Logger.Debugf("Response data:\n%s", response)

	// marshal, encode and write response
	// TODO use MarshalTo with sync.Pool?
	var b []byte
	if b, err = proto.Marshal(response); err != nil {
		return
	}
	rw.Header().Set("Content-Type", "application/x-protobuf")
	rw.Header().Set("Content-Encoding", "snappy")
	dst := *snappyPool.Get().(*[]byte)
	dst = dst[:cap(dst)]
	compressed := snappy.Encode(dst, b)
	_, err = rw.Write(compressed)
	snappyPool.Put(&compressed)
	return
}

func (p *PromAPI) Write(ctx context.Context, rw http.ResponseWriter, req *http.Request) (err error) {
	// track time and response status
	p.mWritesStarted.Inc()
	start := time.Now()
	defer func() {
		p.mWrites.WithLabelValues(errResponseType(err)).Observe(time.Since(start).Seconds())
	}()

	var request prompb.WriteRequest
	if err = readPB(req, &request); err != nil {
		return
	}
	err = p.Storage.Write(ctx, &request)
	return
}

// check interface
var _ prometheus.Collector = (*PromAPI)(nil)
