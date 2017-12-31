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

package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "github.com/kshvakov/clickhouse" // register SQL driver
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages/base"
)

const (
	namespace = "promhouse"
	subsystem = "clickhouse"

	sampleRowSize = 2 + 8 + 8 + 8
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db       *sql.DB
	l        *logrus.Entry
	database string

	timeSeriesRW sync.RWMutex
	timeSeries   map[uint64][]*prompb.Label

	mTimeSeriesCurrent          prometheus.Gauge
	mSamplesCurrent             prometheus.Gauge
	mSamplesCurrentBytes        prometheus.Gauge
	mSamplesCurrentVirtualBytes prometheus.Gauge

	mReads      prometheus.Summary
	mReadErrors *prometheus.CounterVec

	mWrites            prometheus.Summary
	mWriteErrors       *prometheus.CounterVec
	mWrittenTimeSeries prometheus.Counter
	mWrittenSamples    prometheus.Counter
}

func New(dsn string, database string, drop bool) (base.Storage, error) {
	l := logrus.WithField("component", "clickhouse")

	var queries []string
	if drop {
		queries = append(queries, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, database))
	}
	queries = append(queries, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database))

	// TODO use GraphiteMergeTree?
	// change sampleRowSize is you change this table
	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.samples (
			date Date,
			fingerprint UInt64,
			timestamp_ms Int64,
			value Float64
		)
		ENGINE = MergeTree(date, (fingerprint, timestamp_ms), 8192)`, database))

	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.time_series (
			date Date,
			fingerprint UInt64,
			labels String
		)
		ENGINE = ReplacingMergeTree(date, fingerprint, 8192)`, database))

	// we can't use database in DSN if it doesn't yet exist, so handle that in a special way

	dsnURL, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if dsnURL.Query().Get("database") != "" {
		return nil, fmt.Errorf("database should no be set in ClickHouse dsn")
	}

	// init schema
	initDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	defer initDB.Close()
	for _, q := range queries {
		l.Infof("Executing: %s", q)
		if _, err = initDB.Exec(q); err != nil {
			return nil, err
		}
	}

	// reconnect to created database
	q := dsnURL.Query()
	q.Set("database", database)
	dsnURL.RawQuery = q.Encode()
	dsn = dsnURL.String()
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}

	ch := &clickHouse{
		db:       db,
		l:        l,
		database: database,

		timeSeries: make(map[uint64][]*prompb.Label, 8192),

		mTimeSeriesCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "time_series_current",
			Help:      "Current number of stored time series (rows).",
		}),
		mSamplesCurrent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "samples_current",
			Help:      "Current number of stored samples.",
		}),
		mSamplesCurrentBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "samples_current_bytes",
			Help:      "Current number of stored samples (bytes).",
		}),
		mSamplesCurrentVirtualBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "samples_current_virtual_bytes",
			Help:      "Current number of stored samples (virtual uncompressed bytes).",
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
		mWrittenTimeSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_time_series",
			Help:      "Number of written time series.",
		}),
		mWrittenSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_samples",
			Help:      "Number of written samples.",
		}),
	}

	go ch.runTimeSeriesReloader(context.TODO())

	ch.mReadErrors.WithLabelValues("canceled").Set(0)
	ch.mReadErrors.WithLabelValues("other").Set(0)
	ch.mWriteErrors.WithLabelValues("canceled").Set(0)
	ch.mWriteErrors.WithLabelValues("other").Set(0)

	return ch, nil
}

func (ch *clickHouse) runTimeSeriesReloader(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	q := fmt.Sprintf(`SELECT DISTINCT fingerprint, labels FROM %s.time_series`, ch.database)
	for {
		ch.timeSeriesRW.RLock()
		timeSeries := make(map[uint64][]*prompb.Label, len(ch.timeSeries))
		ch.timeSeriesRW.RUnlock()

		err := func() error {
			ch.l.Debug(q)
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
				if timeSeries[f], err = unmarshalLabels(b); err != nil {
					return err
				}
			}
			return rows.Err()
		}()
		if err == nil {
			ch.timeSeriesRW.Lock()
			n := len(timeSeries) - len(ch.timeSeries)
			for f, m := range timeSeries {
				ch.timeSeries[f] = m
			}
			ch.timeSeriesRW.Unlock()
			ch.l.Debugf("Loaded %d existing time series, %d were unknown to this instance.", len(timeSeries), n)
		} else {
			ch.l.Error(err)
		}

		select {
		case <-ctx.Done():
			ch.l.Warn(ctx.Err)
			return
		case <-ticker.C:
		}
	}
}

func (ch *clickHouse) Describe(c chan<- *prometheus.Desc) {
	ch.mTimeSeriesCurrent.Describe(c)
	ch.mSamplesCurrent.Describe(c)
	ch.mSamplesCurrentBytes.Describe(c)
	ch.mSamplesCurrentVirtualBytes.Describe(c)

	ch.mReads.Describe(c)
	ch.mReadErrors.Describe(c)

	ch.mWrites.Describe(c)
	ch.mWriteErrors.Describe(c)
	ch.mWrittenTimeSeries.Describe(c)
	ch.mWrittenSamples.Describe(c)
}

func (ch *clickHouse) Collect(c chan<- prometheus.Metric) {
	// TODO remove this when https://github.com/f1yegor/clickhouse_exporter/pull/13 is merged
	// 'SELECT COUNT(*) FROM samples' is slow
	query := `
	SELECT table, sum(rows) AS rows, sum(bytes) AS bytes, (? * rows) AS virtual_bytes
		FROM system.parts
		WHERE database = ? AND active
		GROUP BY table`
	ch.l.Debugf("%s [%v, %v]", query, sampleRowSize, ch.database)
	rows, err := ch.db.Query(query, sampleRowSize, ch.database)
	if err != nil {
		ch.l.Error(err)
		return
	}
	defer rows.Close()

	var table string
	var r, b, vb uint64
	for rows.Next() {
		if err = rows.Scan(&table, &r, &b, &vb); err != nil {
			ch.l.Error(err)
			return
		}
		switch table {
		case "time_series":
			ch.mTimeSeriesCurrent.Set(float64(r))
			// ignore b and vb
		case "samples":
			ch.mSamplesCurrent.Set(float64(r))
			ch.mSamplesCurrentBytes.Set(float64(b))
			ch.mSamplesCurrentVirtualBytes.Set(float64(vb))
		default:
			ch.l.Errorf("unexpected table %q", table)
		}
	}
	if err = rows.Err(); err != nil {
		ch.l.Error(err)
	}

	ch.mTimeSeriesCurrent.Collect(c)
	ch.mSamplesCurrent.Collect(c)
	ch.mSamplesCurrentBytes.Collect(c)
	ch.mSamplesCurrentVirtualBytes.Collect(c)

	ch.mReads.Collect(c)
	ch.mReadErrors.Collect(c)

	ch.mWrites.Collect(c)
	ch.mWriteErrors.Collect(c)
	ch.mWrittenTimeSeries.Collect(c)
	ch.mWrittenSamples.Collect(c)
}

func (ch *clickHouse) Read(ctx context.Context, queries []base.Query) (res *prompb.ReadResponse, err error) {
	// track time and response status
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

	// special case for {{job="rawsql", query="SELECT …"}} (start is ignored)
	if len(queries) == 1 && len(queries[0].Matchers) == 2 {
		var query string
		var hasJob bool
		for _, m := range queries[0].Matchers {
			if m.Type == base.MatchEqual && m.Name == "job" && m.Value == "rawsql" {
				hasJob = true
			}
			if m.Type == base.MatchEqual && m.Name == "query" {
				query = m.Value
			}
		}
		if hasJob && query != "" {
			return ch.readRawSQL(ctx, query, int64(queries[0].End))
		}
	}

	res = &prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(queries)),
	}
	for i, q := range queries {
		res.Results[i] = new(prompb.QueryResult)

		// find matching time series
		fingerprints := make(map[uint64]struct{}, 64)
		ch.timeSeriesRW.RLock()
		for f, labels := range ch.timeSeries {
			if q.Matchers.MatchLabels(labels) {
				fingerprints[f] = struct{}{}
			}
		}
		ch.timeSeriesRW.RUnlock()
		if len(fingerprints) == 0 {
			continue
		}

		placeholders := strings.Repeat("?, ", len(fingerprints))
		query := fmt.Sprintf(`
			SELECT fingerprint, timestamp_ms, value
				FROM %s.samples
				WHERE fingerprint IN (%s) AND timestamp_ms >= ? AND timestamp_ms <= ?
				ORDER BY fingerprint, timestamp_ms`,
			ch.database, placeholders[:len(placeholders)-2], // cut last ", "
		)
		args := make([]interface{}, 0, len(fingerprints)+2)
		for f := range fingerprints {
			args = append(args, f)
		}
		args = append(args, int64(q.Start))
		args = append(args, int64(q.End))
		err = func() error {
			ch.l.Debugf("%s %v", query, args)
			rows, err := ch.db.Query(query, args...)
			if err != nil {
				return errors.WithStack(err)
			}
			defer rows.Close()

			var ts *prompb.TimeSeries
			var fingerprint, prevFingerprint uint64
			var timestampMs int64
			var value float64
			for rows.Next() {
				if err = rows.Scan(&fingerprint, &timestampMs, &value); err != nil {
					return errors.WithStack(err)
				}
				if fingerprint != prevFingerprint {
					prevFingerprint = fingerprint
					if ts != nil {
						res.Results[i].TimeSeries = append(res.Results[i].TimeSeries, ts)
					}

					ch.timeSeriesRW.RLock()
					labels := ch.timeSeries[fingerprint]
					ch.timeSeriesRW.RUnlock()
					ts = &prompb.TimeSeries{
						Labels: labels,
					}
				}
				ts.Samples = append(ts.Samples, &prompb.Sample{
					TimestampMs: timestampMs,
					Value:       value,
				})
			}
			if ts != nil {
				res.Results[i].TimeSeries = append(res.Results[i].TimeSeries, ts)
			}
			return errors.WithStack(rows.Err())
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

func (ch *clickHouse) Write(ctx context.Context, data *prompb.WriteRequest) (err error) {
	// track time and response status
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

	// calculate fingerprints, map them to time series
	fingerprints := make([]uint64, len(data.TimeSeries))
	timeSeries := make(map[uint64][]*prompb.Label, len(data.TimeSeries))
	for i, ts := range data.TimeSeries {
		base.SortLabels(ts.Labels)
		f := base.Fingerprint(ts.Labels)
		fingerprints[i] = f
		timeSeries[f] = ts.Labels
	}
	if len(fingerprints) != len(timeSeries) {
		ch.l.Debugf("got %d fingerprints, but only %d of them were unique time series", len(fingerprints), len(timeSeries))
	}

	// find new time series
	var newTimeSeries []uint64
	ch.timeSeriesRW.Lock()
	for f, m := range timeSeries {
		_, ok := ch.timeSeries[f]
		if !ok {
			newTimeSeries = append(newTimeSeries, f)
			ch.timeSeries[f] = m
		}
	}
	ch.timeSeriesRW.Unlock()

	// write new time series
	if len(newTimeSeries) > 0 {
		err = inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
			query := fmt.Sprintf(`INSERT INTO %s.time_series (date, fingerprint, labels) VALUES (?, ?, ?)`, ch.database)
			var stmt *sql.Stmt
			if stmt, err = tx.PrepareContext(ctx, query); err != nil {
				return errors.WithStack(err)
			}

			args := make([]interface{}, 3)
			args[0] = model.Now().Time()
			for _, f := range newTimeSeries {
				args[1] = f
				args[2] = marshalLabels(timeSeries[f], make([]byte, 0, 128)) // TODO use pool?
				ch.l.Debugf("%s %v", query, args)
				if _, err = stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
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
		query := fmt.Sprintf(`INSERT INTO %s.samples (date, fingerprint, timestamp_ms, value) VALUES (?, ?, ?, ?)`, ch.database)
		var stmt *sql.Stmt
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return errors.WithStack(err)
		}

		args := make([]interface{}, 4)
		for i, ts := range data.TimeSeries {
			args[1] = fingerprints[i]

			for _, s := range ts.Samples {
				args[0] = model.Time(s.TimestampMs).Time()
				args[2] = s.TimestampMs
				args[3] = s.Value
				ch.l.Debugf("%s %v", query, args)
				if _, err = stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
				}
				samples++
			}
		}

		return stmt.Close()
	})
	if err != nil {
		return
	}

	ch.mWrittenTimeSeries.Add(float64(len(newTimeSeries)))
	ch.mWrittenSamples.Add(float64(samples))
	ch.l.Debugf("Wrote %d new time series, %d samples.", len(newTimeSeries), samples)
	return
}

// check interface
var _ base.Storage = (*clickHouse)(nil)
