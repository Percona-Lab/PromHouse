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
	"runtime/pprof"
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

		queries[i] = q
	}

	return queries
}

// errResponseType converts given error to short string used as metric label value.
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

// wrap wraps API handler with logging and profiling.
func (p *PromAPI) wrap(h func(http.ResponseWriter, *http.Request) (time.Duration, error)) http.HandlerFunc {
	return func(rw http.ResponseWriter, req *http.Request) {
		// add profile labels to request's context
		ctx := req.Context()
		labels := pprof.Labels("path", req.URL.Path)
		pprof.Do(ctx, labels, func(ctx context.Context) {
			req = req.WithContext(ctx)

			dur, err := h(rw, req)
			dur = dur.Truncate(time.Millisecond)

			if err != nil {
				http.Error(rw, err.Error(), 400)
				p.l.Errorf("%s %s -> 400, %s (%s)", req.Method, req.URL, err, dur)
				return
			}
			p.l.Infof("%s %s -> 200 (%s)", req.Method, req.URL, dur)
		})
	}
}

// Read implements Prometheus Remote API Read call.
func (p *PromAPI) Read(http.ResponseWriter, *http.Request) { p.wrap(p.read) }

func (p *PromAPI) read(rw http.ResponseWriter, req *http.Request) (dur time.Duration, err error) {
	// track time and response status
	p.mReadsStarted.Inc()
	start := time.Now()
	defer func() {
		dur = time.Since(start)
		p.mReads.WithLabelValues(errResponseType(err)).Observe(dur.Seconds())
	}()

	var request prompb.ReadRequest
	if err = readRequest(req, &request); err != nil {
		return
	}

	// read from storage
	queries := p.convertReadRequest(&request)
	for i, q := range queries {
		p.l.Infof("Query %d: %s", i+1, q)
	}
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

// Write implements Prometheus Remote API Write call.
func (p *PromAPI) Write(http.ResponseWriter, *http.Request) { p.wrap(p.write) }

func (p *PromAPI) write(rw http.ResponseWriter, req *http.Request) (dur time.Duration, err error) {
	// track time and response status
	p.mWritesStarted.Inc()
	start := time.Now()
	defer func() {
		dur = time.Since(start)
		p.mWrites.WithLabelValues(errResponseType(err)).Observe(dur.Seconds())
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

	if err == nil {
		p.l.Debugf("Wrote %d samples.", samples)
	} else {
		p.l.Errorf("Error writing %d samples: %s.", samples, err)
	}

	return
}

// check interface
var _ prometheus.Collector = (*PromAPI)(nil)
