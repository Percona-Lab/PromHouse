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
	"time"

	_ "github.com/kshvakov/clickhouse" // register SQL driver
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
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
	// labels   []string

	mSamplesCurrent prometheus.Gauge

	mReads      prometheus.Summary
	mReadErrors *prometheus.CounterVec

	mWrites         prometheus.Summary
	mWriteErrors    *prometheus.CounterVec
	mWrittenLabels  *prometheus.CounterVec
	mWrittenSamples prometheus.Counter
}

func NewClickHouse(dsn string, database string, init bool) (*ClickHouse, error) {
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

	for _, q := range queries {
		if _, err = db.Exec(q); err != nil {
			return nil, err
		}
	}

	return &ClickHouse{
		db:       db,
		l:        logrus.WithField("component", "clickhouse"),
		database: database,
		// labels:   labels,

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
		mWrittenSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_samples",
			Help:      "Number of written samples.",
		}),
	}, nil
}

func (ch *ClickHouse) Describe(c chan<- *prometheus.Desc) {
	ch.mSamplesCurrent.Describe(c)

	ch.mReads.Describe(c)
	ch.mReadErrors.Describe(c)

	ch.mWrites.Describe(c)
	ch.mWriteErrors.Describe(c)
	ch.mWrittenLabels.Describe(c)
	ch.mWrittenSamples.Describe(c)
}

func (ch *ClickHouse) Collect(c chan<- prometheus.Metric) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.samples", ch.database)
	var count uint64
	if err := ch.db.QueryRow(query).Scan(&count); err != nil {
		ch.l.Error(err)
		return
	}
	ch.mSamplesCurrent.Set(float64(count))

	ch.mSamplesCurrent.Collect(c)

	ch.mReads.Collect(c)
	ch.mReadErrors.Collect(c)

	ch.mWrites.Collect(c)
	ch.mWriteErrors.Collect(c)
	ch.mWrittenLabels.Collect(c)
	ch.mWrittenSamples.Collect(c)
}

func (ch *ClickHouse) Read(ctx context.Context, queries []Query) (data []model.Matrix, err error) {
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

	data = make([]model.Matrix, len(queries))
	for i, q := range queries {
		query := fmt.Sprintf("SELECT __labels__, __fingerprint__, __ts__, __value__ FROM %s.samples WHERE __ts__ >= ? AND __ts__ <= ?", ch.database)
		args := []interface{}{int64(q.Start), int64(q.End)}
		for _, matcher := range q.Matchers {
			// if matcher.Name == model.MetricNameLabel {
			// 	query += fmt.Sprintf(" AND %s", matcher.Name)
			// } else {

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

		var ss *model.SampleStream
		var b []byte
		var fingerprint, prevFingerprint uint64
		var ts int64
		var value float64
		for rows.Next() {
			if err = rows.Scan(&b, &fingerprint, &ts, &value); err != nil {
				return
			}
			if fingerprint != prevFingerprint {
				prevFingerprint = fingerprint
				if ss != nil {
					data[i] = append(data[i], ss)
				}

				var ls model.LabelSet
				if err = ls.UnmarshalJSON(b); err != nil {
					return
				}
				ss = &model.SampleStream{
					Metric: model.Metric(ls),
				}
			}
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(ts),
				Value:     model.SampleValue(value),
			})
		}
		if err = rows.Close(); err != nil {
			return
		}
		if ss != nil {
			data[i] = append(data[i], ss)
		}
	}

	return
}

func (ch *ClickHouse) Write(ctx context.Context, data model.Matrix) (err error) {
	start := time.Now()
	var tx *sql.Tx
	defer func() {
		if err == nil {
			ch.mWrites.Observe(time.Since(start).Seconds())
			return
		}

		if tx != nil {
			tx.Rollback()
		}

		t := "other"
		if err == context.Canceled {
			t = "canceled"
		}
		ch.mWriteErrors.WithLabelValues(t).Inc()
	}()

	tx, err = ch.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}

	// columns := 4 + len(ch.labels)
	const columns = 5
	placeholders := strings.Repeat("?, ", columns)
	query := fmt.Sprintf(
		`INSERT INTO %s.samples (__labels__, __fingerprint__, __ts__, __value__, __date__) VALUES (%s)`,
		ch.database, placeholders[:len(placeholders)-2], // cut last ", "
	)

	var stmt *sql.Stmt
	if stmt, err = tx.PrepareContext(ctx, query); err != nil {
		return err
	}
	defer func() {
		e := stmt.Close()
		if err == nil {
			err = e
		}
	}()

	for _, ss := range data {
		args := make([]interface{}, columns)

		labels := make(map[string]string, len(ss.Metric))
		for n, v := range ss.Metric {
			// if n == model.MetricNameLabel {
			// 	args[0] = string(v)
			// } else {
			labels[string(n)] = string(v)
			ch.mWrittenLabels.WithLabelValues(string(n)).Inc()
		}
		args[0] = util.MarshalLabels(labels)
		args[1] = uint64(ss.Metric.Fingerprint())

		// for i, l := range ch.labels {
		// 	args[i+1] = string(ss.Metric[model.LabelName(l)])
		// }

		for _, sp := range ss.Values {
			args[len(args)-3] = int64(sp.Timestamp)
			args[len(args)-2] = float64(sp.Value)
			args[len(args)-1] = sp.Timestamp.Time()
			_, err = stmt.ExecContext(ctx, args...)
			if err != nil {
				return
			}
		}
	}

	if err = tx.Commit(); err != nil {
		return
	}
	ch.mWrittenSamples.Add(float64(len(data)))
	return
}

// check interface
var _ Storage = (*ClickHouse)(nil)
