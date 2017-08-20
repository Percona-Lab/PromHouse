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
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/kshvakov/clickhouse" // register SQL driver
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/util"
)

const (
	namespace = "promhouse"
	subsystem = "clickhouse"
)

// ClickHouse implements storage interface for the ClickHouse.
type ClickHouse struct {
	db       *sql.DB
	l        *logrus.Entry
	database string

	metrics   map[model.Fingerprint]model.Metric
	metricsRW sync.RWMutex

	mMetricsCurrent prometheus.Gauge
	mSamplesCurrent prometheus.Gauge

	mReads      prometheus.Summary
	mReadErrors *prometheus.CounterVec

	mWrites         prometheus.Summary
	mWriteErrors    *prometheus.CounterVec
	mWrittenLabels  *prometheus.CounterVec
	mWrittenMetrics prometheus.Counter
	mWrittenSamples prometheus.Counter
}

func NewClickHouse(dsn string, database string, init bool) (*ClickHouse, error) {
	l := logrus.WithField("component", "clickhouse")

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	var queries []string
	if init {
		queries = append(queries, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, database))
	}
	queries = append(queries, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database))

	// TODO use GraphiteMergeTree?
	// TODO remove fingerprint?
	// TODO add __name__, instance, job as separate columns?
	// labels := []string{"instance", "job"}
	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.samples (
			__date__ Date,
			__labels__ String,
			__fingerprint__ UInt64,
			__ts__ Int64,
			__value__ Float64
		)
		ENGINE = MergeTree(__date__, (__fingerprint__, __ts__), 8192)`, database))

	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.metrics (
			date Date,
			fingerprint UInt64,
			labels String
		)
		ENGINE = MergeTree(date, fingerprint, 8192)`, database))

	for _, q := range queries {
		l.Infof("Executing: %s", q)
		if _, err = db.Exec(q); err != nil {
			return nil, err
		}
	}

	ch := &ClickHouse{
		db:       db,
		l:        l,
		database: database,

		metrics: make(map[model.Fingerprint]model.Metric, 8192),

		mMetricsCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "metrics_current",
			Help:      "Current number of stored metrics.",
		}),
		mSamplesCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "samples_current",
			Help:      "Current number of stored samples.",
		}),

		mReads: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "reads",
			Help:      "Durations of successful reads.",
		}),
		mReadErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_errors",
			Help:      "Number of read errors by type: canceled, other.",
		}, []string{"type"}),

		mWrites: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "writes",
			Help:      "Durations of committed writes.",
		}),
		mWriteErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "write_errors",
			Help:      "Number of write errors by type: canceled, other.",
		}, []string{"type"}),
		mWrittenLabels: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_labels",
			Help:      "Number of written labels by name.",
		}, []string{"name"}),
		mWrittenMetrics: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_metrics",
			Help:      "Number of written metrics.",
		}),
		mWrittenSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_samples",
			Help:      "Number of written samples.",
		}),
	}

	ch.mReadErrors.WithLabelValues("canceled").Set(0)
	ch.mReadErrors.WithLabelValues("other").Set(0)
	ch.mWriteErrors.WithLabelValues("canceled").Set(0)
	ch.mWriteErrors.WithLabelValues("other").Set(0)

	return ch, nil
}

// makeMetric converts a slice of labels to a metric.
func makeMetric(labels []*prompb.Label) model.Metric {
	metric := make(model.Metric, len(labels))
	for _, l := range labels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}
	return metric
}

func (ch *ClickHouse) Describe(c chan<- *prometheus.Desc) {
	ch.mMetricsCurrent.Describe(c)
	ch.mSamplesCurrent.Describe(c)

	ch.mReads.Describe(c)
	ch.mReadErrors.Describe(c)

	ch.mWrites.Describe(c)
	ch.mWriteErrors.Describe(c)
	ch.mWrittenLabels.Describe(c)
	ch.mWrittenMetrics.Describe(c)
	ch.mWrittenSamples.Describe(c)
}

func (ch *ClickHouse) Collect(c chan<- prometheus.Metric) {
	var count uint64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.metrics", ch.database)
	if err := ch.db.QueryRow(query).Scan(&count); err != nil {
		ch.l.Error(err)
		return
	}
	ch.mMetricsCurrent.Set(float64(count))

	query = fmt.Sprintf("SELECT COUNT(*) FROM %s.samples", ch.database)
	if err := ch.db.QueryRow(query).Scan(&count); err != nil {
		ch.l.Error(err)
		return
	}
	ch.mSamplesCurrent.Set(float64(count))

	ch.mMetricsCurrent.Collect(c)
	ch.mSamplesCurrent.Collect(c)

	ch.mReads.Collect(c)
	ch.mReadErrors.Collect(c)

	ch.mWrites.Collect(c)
	ch.mWriteErrors.Collect(c)
	ch.mWrittenLabels.Collect(c)
	ch.mWrittenMetrics.Collect(c)
	ch.mWrittenSamples.Collect(c)
}

func (ch *ClickHouse) Read(ctx context.Context, queries []Query) (res *prompb.ReadResponse, err error) {
	start := time.Now()
	defer func() {
		if err == nil {
			ch.mReads.Observe(time.Since(start).Seconds())
			return
		}

		t := "other"
		if err == context.Canceled {
			t = "canceled"
		}
		ch.mReadErrors.WithLabelValues(t).Inc()
	}()

	res = &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(queries)),
	}
	for i, q := range queries {
		res.Results[i] = new(prompb.QueryResult)
		query := fmt.Sprintf("SELECT __labels__, __fingerprint__, __ts__, __value__ FROM %s.samples WHERE __ts__ >= ? AND __ts__ <= ?", ch.database)
		args := []interface{}{int64(q.Start), int64(q.End)}
		for _, matcher := range q.Matchers {
			switch matcher.Type {
			case MatchEqual:
				query += fmt.Sprintf(" AND visitParamExtractString(__labels__, '%s') = ?", matcher.Name)
				args = append(args, matcher.Value)
			case MatchNotEqual:
				query += fmt.Sprintf(" AND visitParamExtractString(__labels__, '%s') != ?", matcher.Name)
				args = append(args, matcher.Value)
			case MatchRegexp:
				query += fmt.Sprintf(" AND match(visitParamExtractString(__labels__, '%s'), ?)", matcher.Name)
				args = append(args, "^(?:"+matcher.Value+")$")
			case MatchNotRegexp:
				query += fmt.Sprintf(" AND NOT match(visitParamExtractString(__labels__, '%s'), ?)", matcher.Name)
				args = append(args, "^(?:"+matcher.Value+")$")
			default:
				ch.l.Panicf("unexpected match type: %d", matcher.Type)
			}
		}
		query += " ORDER BY __fingerprint__, __ts__"

		var rows *sql.Rows
		rows, err = ch.db.Query(query, args...)
		if err != nil {
			return
		}

		var ts *prompb.TimeSeries
		var b []byte
		var fingerprint, prevFingerprint uint64
		var t int64
		var value float64
		for rows.Next() {
			if err = rows.Scan(&b, &fingerprint, &t, &value); err != nil {
				return
			}
			if fingerprint != prevFingerprint {
				prevFingerprint = fingerprint
				if ts != nil {
					res.Results[i].Timeseries = append(res.Results[i].Timeseries, ts)
				}

				var ls model.LabelSet
				if err = ls.UnmarshalJSON(b); err != nil {
					return
				}
				labels := make([]*prompb.Label, 0, len(ls))
				for n, v := range ls {
					labels = append(labels, &prompb.Label{
						Name:  string(n),
						Value: string(v),
					})
				}
				ts = &prompb.TimeSeries{
					Labels: labels,
				}
			}
			ts.Samples = append(ts.Samples, &prompb.Sample{
				Timestamp: t,
				Value:     value,
			})
		}
		if err = rows.Close(); err != nil {
			return
		}
		if ts != nil {
			res.Results[i].Timeseries = append(res.Results[i].Timeseries, ts)
		}
	}

	return
}

func inTransaction(ctx context.Context, db *sql.DB, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = db.BeginTx(ctx, nil); err != nil {
		return
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()
	err = f(tx)
	return
}

func (ch *ClickHouse) Write(ctx context.Context, data *prompb.WriteRequest) (err error) {
	start := time.Now()
	defer func() {
		if err == nil {
			ch.mWrites.Observe(time.Since(start).Seconds())
			return
		}

		t := "other"
		if err == context.Canceled {
			t = "canceled"
		}
		ch.mWriteErrors.WithLabelValues(t).Inc()
	}()

	// make metrics and calculate fingerprints
	metrics := make(map[model.Fingerprint]model.Metric, len(data.Timeseries))
	for _, ts := range data.Timeseries {
		m := makeMetric(ts.Labels)
		metrics[m.Fingerprint()] = m
	}

	// find new metrics
	newMetrics := make([]model.Fingerprint, 0, len(metrics))
	ch.metricsRW.Lock()
	for f, m := range metrics {
		_, ok := ch.metrics[f]
		if !ok {
			newMetrics = append(newMetrics, f)
			ch.metrics[f] = m
		}
	}
	ch.metricsRW.Unlock()

	// write new metrics
	err = inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
		placeholders := strings.Repeat("?, ", 3)
		query := fmt.Sprintf(
			`INSERT INTO %s.metrics (date, fingerprint, labels) VALUES (%s)`,
			ch.database, placeholders[:len(placeholders)-2], // cut last ", "
		)
		var stmt *sql.Stmt
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return err
		}

		args := make([]interface{}, 3)
		args[0] = model.Now().Time()
		for _, f := range newMetrics {
			args[1] = uint64(f)
			args[2] = util.MarshalMetric(metrics[f])
			if _, err = stmt.ExecContext(ctx, args...); err != nil {
				return err
			}
		}

		return stmt.Close()
	})
	if err != nil {
		return
	}

	// write samples
	var samples int
	err = inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
		placeholders := strings.Repeat("?, ", 5)
		query := fmt.Sprintf(
			`INSERT INTO %s.samples (__labels__, __fingerprint__, __ts__, __value__, __date__) VALUES (%s)`,
			ch.database, placeholders[:len(placeholders)-2], // cut last ", "
		)
		var stmt *sql.Stmt
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return err
		}

		args := make([]interface{}, 5)
		for _, ts := range data.Timeseries {
			m := makeMetric(ts.Labels)
			args[0] = util.MarshalMetric(m)
			args[1] = uint64(m.Fingerprint())

			for _, s := range ts.Samples {
				args[len(args)-3] = s.Timestamp
				args[len(args)-2] = s.Value
				args[len(args)-1] = model.Time(s.Timestamp).Time()
				if _, err = stmt.ExecContext(ctx, args...); err != nil {
					return err
				}
				samples++
			}
		}

		return stmt.Close()
	})
	if err != nil {
		return
	}

	ch.mWrittenMetrics.Add(float64(len(newMetrics)))
	ch.mWrittenSamples.Add(float64(samples))
	ch.l.Debugf("Wrote %s new metrics, %d samples.", len(newMetrics), samples)
	return
}

// check interface
var _ Storage = (*ClickHouse)(nil)
