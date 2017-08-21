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
	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.samples (
			date Date,
			fingerprint UInt64,
			timestamp Int64,
			value Float64
		)
		ENGINE = MergeTree(date, (fingerprint, timestamp), 8192)`, database))

	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.metrics (
			date Date,
			fingerprint UInt64,
			labels String
		)
		ENGINE = ReplacingMergeTree(date, fingerprint, 8192)`, database))

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

	go ch.runMetricsReloader(context.TODO())

	ch.mReadErrors.WithLabelValues("canceled").Set(0)
	ch.mReadErrors.WithLabelValues("other").Set(0)
	ch.mWriteErrors.WithLabelValues("canceled").Set(0)
	ch.mWriteErrors.WithLabelValues("other").Set(0)

	return ch, nil
}

func (ch *ClickHouse) runMetricsReloader(ctx context.Context) {
	ticker := time.Tick(time.Second)
	q := fmt.Sprintf(`SELECT DISTINCT fingerprint, labels FROM %s.metrics`, ch.database)
	for {
		ch.metricsRW.RLock()
		metrics := make(map[model.Fingerprint]model.Metric, len(ch.metrics))
		ch.metricsRW.RUnlock()

		err := func() error {
			rows, err := ch.db.Query(q)
			if err != nil {
				return err
			}
			defer rows.Close()

			var f uint64
			var b []byte
			for rows.Next() {
				if err = rows.Scan(&f, &b); err != nil {
					return err
				}
				var ls model.LabelSet
				if err = ls.UnmarshalJSON(b); err != nil {
					return err
				}
				metrics[model.Fingerprint(f)] = model.Metric(ls)
			}
			return rows.Err()
		}()
		if err == nil {
			ch.metricsRW.Lock()
			n := len(metrics) - len(ch.metrics)
			for f, m := range metrics {
				ch.metrics[f] = m
			}
			ch.metricsRW.Unlock()
			ch.l.Debugf("Loaded %d existing metrics, %d were unknown to this instance.", len(metrics), n)
		} else {
			ch.l.Error(err)
		}

		select {
		case <-ctx.Done():
			ch.l.Warn(ctx.Err)
			return
		case <-ticker:
		}
	}
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
	ch.mWrittenMetrics.Describe(c)
	ch.mWrittenSamples.Describe(c)
}

func (ch *ClickHouse) Collect(c chan<- prometheus.Metric) {
	// TODO remove this when https://github.com/f1yegor/clickhouse_exporter/pull/12 is merged
	// 'SELECT COUNT(*) FROM samples' is slow
	query := `
	SELECT table, sum(rows) AS rows
		FROM system.parts
		WHERE database = ? AND active
		GROUP BY table`
	rows, err := ch.db.Query(query, ch.database)
	if err != nil {
		ch.l.Error(err)
		return
	}
	defer rows.Close()

	var table string
	var sum uint64
	for rows.Next() {
		if err = rows.Scan(&table, &sum); err != nil {
			ch.l.Error(err)
			return
		}
		switch table {
		case "metrics":
			ch.mMetricsCurrent.Set(float64(sum))
		case "samples":
			ch.mSamplesCurrent.Set(float64(sum))
		default:
			ch.l.Errorf("unexpected table %q", table)
		}
	}
	if err = rows.Err(); err != nil {
		ch.l.Error(err)
	}

	ch.mMetricsCurrent.Collect(c)
	ch.mSamplesCurrent.Collect(c)

	ch.mReads.Collect(c)
	ch.mReadErrors.Collect(c)

	ch.mWrites.Collect(c)
	ch.mWriteErrors.Collect(c)
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

		// find matching metrics
		fingerprints := make(map[uint64]struct{}, 64)
		ch.metricsRW.RLock()
		for f, m := range ch.metrics {
			if q.Matchers.Match(m) {
				fingerprints[uint64(f)] = struct{}{}
			}
		}
		ch.metricsRW.RUnlock()
		if len(fingerprints) == 0 {
			continue
		}

		// TODO check why uint64 is not properly handled, create an issue
		var fs string
		for f := range fingerprints {
			fs += fmt.Sprintf("%d, ", f)
		}

		query := fmt.Sprintf(`
			SELECT fingerprint, timestamp, value
				FROM %s.samples
				WHERE fingerprint IN (%s) AND timestamp >= ? AND timestamp <= ?
				ORDER BY fingerprint, timestamp`,
			ch.database, fs[:len(fs)-2], // cut last ", "
		)
		args := []interface{}{int64(q.Start), int64(q.End)}
		err = func() error {
			rows, err := ch.db.Query(query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			var ts *prompb.TimeSeries
			var fingerprint, prevFingerprint uint64
			var timestamp int64
			var value float64
			for rows.Next() {
				if err = rows.Scan(&fingerprint, &timestamp, &value); err != nil {
					return err
				}
				if fingerprint != prevFingerprint {
					prevFingerprint = fingerprint
					if ts != nil {
						res.Results[i].Timeseries = append(res.Results[i].Timeseries, ts)
					}

					ch.metricsRW.RLock()
					m := ch.metrics[model.Fingerprint(fingerprint)]
					ch.metricsRW.RUnlock()
					labels := make([]*prompb.Label, 0, len(m))
					for n, v := range m {
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
					Timestamp: timestamp,
					Value:     value,
				})
			}
			if ts != nil {
				res.Results[i].Timeseries = append(res.Results[i].Timeseries, ts)
			}
			return rows.Err()
		}()
		if err != nil {
			return
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
	timeseries := make([]model.Fingerprint, len(data.Timeseries))
	metrics := make(map[model.Fingerprint]model.Metric, len(data.Timeseries))
	for i, ts := range data.Timeseries {
		m := makeMetric(ts.Labels)
		f := m.Fingerprint()
		timeseries[i] = f
		metrics[f] = m
	}
	if len(timeseries) != len(metrics) {
		ch.l.Debugf("got %d timeseries, but only %d of them are unique metrics", len(timeseries), len(metrics))
	}

	// find new metrics
	var newMetrics []model.Fingerprint
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
	if len(newMetrics) > 0 {
		err = inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
			query := fmt.Sprintf(`INSERT INTO %s.metrics (date, fingerprint, labels) VALUES (?, ?, ?)`, ch.database)
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
	}

	// write samples
	var samples int
	err = inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
		query := fmt.Sprintf(`INSERT INTO %s.samples (date, fingerprint, timestamp, value) VALUES (?, ?, ?, ?)`, ch.database)
		var stmt *sql.Stmt
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return err
		}

		args := make([]interface{}, 4)
		for i, ts := range data.Timeseries {
			args[1] = uint64(timeseries[i])

			for _, s := range ts.Samples {
				args[0] = model.Time(s.Timestamp).Time()
				args[2] = s.Timestamp
				args[3] = s.Value
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
	ch.l.Debugf("Wrote %d new metrics, %d samples.", len(newMetrics), samples)
	return
}

// check interface
var _ Storage = (*ClickHouse)(nil)
