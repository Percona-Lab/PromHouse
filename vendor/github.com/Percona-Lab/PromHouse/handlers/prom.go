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

// Package handlers provides Prometheus Remote API handlers.
package handlers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
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

// PromAPI provides Prometheus Remote API handlers.
type PromAPI struct {
	storage base.Storage
	l       *logrus.Entry

	mReadsStarted prometheus.Counter
	mReads        *prometheus.SummaryVec

	mWritesStarted  prometheus.Counter
	mWrites         *prometheus.SummaryVec
	mWrittenSamples prometheus.Counter
}

// NewPromAPI creates a new PromAPI instance.
func NewPromAPI(storage base.Storage, l *logrus.Entry) *PromAPI {
	return &PromAPI{
		storage: storage,
		l:       l,

		mReadsStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "reads_started",
			Help:      "Number of started reads.",
		}),
		mReads: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "reads",
			Help:      "Durations of reads by result type: ok, canceled, other.",
			MaxAge:    time.Minute,
		}, []string{"type"}),

		mWritesStarted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "writes_started",
			Help:      "Number of started writes.",
		}),
		mWrites: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "writes",
			Help:      "Durations of writes by result type: ok, canceled, other.",
			MaxAge:    time.Minute,
		}, []string{"type"}),
		mWrittenSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_samples",
			Help:      "Number of written samples.",
		}),
	}
}

// Describe sends the super-set of all possible descriptors of metrics
// collected by this Collector to the provided channel and returns once
// the last descriptor has been sent.
//
// It implements prometheus.Collector interface.
func (p *PromAPI) Describe(c chan<- *prometheus.Desc) {
	p.mReadsStarted.Describe(c)
	p.mWritesStarted.Describe(c)
	p.mReads.Describe(c)
	p.mWrites.Describe(c)
	p.mWrittenSamples.Describe(c)
}

// Collect is called by the Prometheus registry when collecting
// metrics. The implementation sends each collected metric via the
// provided channel and returns once the last metric has been sent.
//
// It implements prometheus.Collector interface.
func (p *PromAPI) Collect(c chan<- prometheus.Metric) {
	p.mReadsStarted.Collect(c)
	p.mWritesStarted.Collect(c)
	p.mReads.Collect(c)
	p.mWrites.Collect(c)
	p.mWrittenSamples.Collect(c)
}

// Stores pointers, not slices. See https://staticcheck.io/docs/staticcheck#SA6002
var snappyPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024)
		return &b
	},
}

// readRequest reads snappy-compressed protobuf request.
func readRequest(req *http.Request, pb proto.Message) error {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return errors.WithStack(err)
	}

	dst := *snappyPool.Get().(*[]byte)
	dst = dst[:cap(dst)]
	b, err := snappy.Decode(dst, compressed)
	if err == nil {
		err = proto.Unmarshal(b, pb)
	}
	snappyPool.Put(&b)
	return errors.WithStack(err)
}

// convertReadRequest converts protobuf read request into a slice of storage queries.
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
				p.l.Panicf("convertReadRequest: unexpected matcher %d", m.Type)
			}

			q.Matchers[j] = base.Matcher{
				Type:  t,
				Name:  m.Name,
				Value: m.Value,
			}
		}

		if rq.Hints != nil {
			p.l.Warnf("Ignoring hint %+v for query %v.", *rq.Hints, q)
		}

		queries[i] = q
	}

	return queries
}

// errResponseType converts given error to short string used as metric label value.
func errResponseType(err error) string {
	switch errors.Cause(err) {
	case nil:
		return "ok"
	case context.Canceled, context.DeadlineExceeded:
		return "canceled"
	case sql.ErrConnDone, sql.ErrNoRows, sql.ErrTxDone, driver.ErrBadConn:
		return "conn"
	default:
		return "other"
	}
}

// wrap wraps API handler with logging and profiling.
func (p *PromAPI) wrap(h func(http.ResponseWriter, *http.Request) (string, error)) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		// add profile labels to request's context
		ctx := req.Context()
		labels := pprof.Labels("path", req.URL.Path)
		pprof.Do(ctx, labels, func(ctx context.Context) {
			req = req.WithContext(ctx)

			info, err := h(rw, req)

			if err != nil {
				http.Error(rw, err.Error(), 400)
				p.l.Errorf("%s %s -> 400 %s: %+v", req.Method, req.URL, info, err)
				return
			}
			p.l.Infof("%s %s -> 200 %s", req.Method, req.URL, info)
		})
	}
}

// Read returns HTTP handler implementing Prometheus Remote API Read call.
func (p *PromAPI) Read() http.HandlerFunc { return p.wrap(p.read) }

func (p *PromAPI) read(rw http.ResponseWriter, req *http.Request) (info string, err error) {
	// track time and response status
	p.mReadsStarted.Inc()
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		p.mReads.WithLabelValues(errResponseType(err)).Observe(dur.Seconds())
		if info == "" {
			info = dur.Truncate(time.Millisecond).String()
		} else {
			info = fmt.Sprintf("%s, %s", dur.Truncate(time.Millisecond), info)
		}
	}()

	var request prompb.ReadRequest
	if err = readRequest(req, &request); err != nil {
		return
	}

	// read from storage
	queries := p.convertReadRequest(&request)
	info = "queries: "
	for i, q := range queries {
		info += fmt.Sprintf("%d: %s, ", i+1, q)
	}
	info = strings.TrimSuffix(info, ", ")
	var response *prompb.ReadResponse
	if response, err = p.storage.Read(req.Context(), queries); err != nil {
		return
	}
	p.l.Debugf("Response data:\n%s", response)

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

// Write returns HTTP handler implementing Prometheus Remote API Write call.
func (p *PromAPI) Write() http.HandlerFunc { return p.wrap(p.write) }

func (p *PromAPI) write(rw http.ResponseWriter, req *http.Request) (info string, err error) {
	// track time and response status
	p.mWritesStarted.Inc()
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		p.mWrites.WithLabelValues(errResponseType(err)).Observe(dur.Seconds())
		if info == "" {
			info = dur.Truncate(time.Millisecond).String()
		} else {
			info = fmt.Sprintf("%s, %s", dur.Truncate(time.Millisecond), info)
		}
	}()

	var request prompb.WriteRequest
	if err = readRequest(req, &request); err != nil {
		return
	}
	err = p.storage.Write(req.Context(), &request)

	var samples int
	for _, ts := range request.TimeSeries {
		samples += len(ts.Samples)
	}
	p.mWrittenSamples.Add(float64(samples))
	info = fmt.Sprintf("%d samples", samples)
	return
}

// check interfaces
var (
	_ prometheus.Collector = (*PromAPI)(nil)
)
