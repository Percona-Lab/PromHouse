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

// Package clickhouse provides ClickHouse storage.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"runtime/pprof"
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
	"github.com/Percona-Lab/PromHouse/utils/timeseries"
)

const (
	namespace = "promhouse"
	subsystem = "clickhouse"
)

// clickHouse implements storage interface for the ClickHouse.
type clickHouse struct {
	db                   *sql.DB
	l                    *logrus.Entry
	database             string
	maxTimeSeriesInQuery int

	timeSeriesRW sync.RWMutex
	timeSeries   map[uint64][]*prompb.Label

	mWrittenTimeSeries prometheus.Counter
}

type Params struct {
	DSN                  string
	DropDatabase         bool
	MaxOpenConns         int
	MaxTimeSeriesInQuery int
}

func New(params *Params) (base.Storage, error) {
	l := logrus.WithField("component", "clickhouse")

	dsnURL, err := url.Parse(params.DSN)
	if err != nil {
		return nil, err
	}
	database := dsnURL.Query().Get("database")
	if database == "" {
		return nil, fmt.Errorf("database should be set in ClickHouse DSN")
	}

	var queries []string
	if params.DropDatabase {
		queries = append(queries, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, database))
	}
	queries = append(queries, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, database))

	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.time_series (
			date Date,
			fingerprint UInt64,
			labels String
		)
		ENGINE = ReplacingMergeTree
			PARTITION BY date
			ORDER BY fingerprint`, database))

	// change sampleRowSize is you change this table
	queries = append(queries, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.samples (
			fingerprint UInt64,
			timestamp_ms Int64,
			value Float64
		)
		ENGINE = MergeTree
			PARTITION BY toDate(timestamp_ms / 1000)
			ORDER BY (fingerprint, timestamp_ms)`, database))

	// connect without seting database, init schema
	q := dsnURL.Query()
	q.Del("database")
	initURL := dsnURL
	initURL.RawQuery = q.Encode()
	initDB, err := sql.Open("clickhouse", initURL.String())
	if err != nil {
		return nil, err
	}
	defer initDB.Close()
	for _, q := range queries {
		q = strings.TrimSpace(q)
		l.Infof("Executing:\n%s", q)
		if _, err = initDB.Exec(q); err != nil {
			return nil, err
		}
	}

	// reconnect to created database
	db, err := sql.Open("clickhouse", params.DSN)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(params.MaxOpenConns)

	ch := &clickHouse{
		db:                   db,
		l:                    l,
		database:             database,
		maxTimeSeriesInQuery: params.MaxTimeSeriesInQuery,

		timeSeries: make(map[uint64][]*prompb.Label, 8192),

		mWrittenTimeSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "written_time_series",
			Help:      "Number of written time series.",
		}),
	}

	go func() {
		ctx := pprof.WithLabels(context.TODO(), pprof.Labels("component", "clickhouse_reloader"))
		pprof.SetGoroutineLabels(ctx)
		ch.runTimeSeriesReloader(ctx)
	}()

	return ch, nil
}

func (ch *clickHouse) runTimeSeriesReloader(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
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
	ch.mWrittenTimeSeries.Describe(c)
}

func (ch *clickHouse) Collect(c chan<- prometheus.Metric) {
	ch.mWrittenTimeSeries.Collect(c)
}

type beginTxer interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

func inTransaction(ctx context.Context, txer beginTxer, f func(*sql.Tx) error) (err error) {
	var tx *sql.Tx
	if tx, err = txer.BeginTx(ctx, nil); err != nil {
		err = errors.WithStack(err)
		return
	}
	defer func() {
		if err == nil {
			err = errors.WithStack(tx.Commit())
		} else {
			tx.Rollback()
		}
	}()
	err = f(tx)
	return
}

func (ch *clickHouse) scanSamples(rows *sql.Rows) ([]*prompb.TimeSeries, error) {
	// scan results
	var res []*prompb.TimeSeries
	var ts *prompb.TimeSeries
	var fingerprint, prevFingerprint uint64
	var timestampMs int64
	var value float64
	for rows.Next() {
		if err := rows.Scan(&fingerprint, &timestampMs, &value); err != nil {
			return nil, errors.WithStack(err)
		}

		// collect samples in time series
		if fingerprint != prevFingerprint {
			// add collected time series to result
			prevFingerprint = fingerprint
			if ts != nil {
				res = append(res, ts)
			}

			// create new time series
			ch.timeSeriesRW.RLock()
			labels := ch.timeSeries[fingerprint]
			ch.timeSeriesRW.RUnlock()
			ts = &prompb.TimeSeries{
				Labels: labels,
			}
		}

		// add samples to current time series
		ts.Samples = append(ts.Samples, &prompb.Sample{
			TimestampMs: timestampMs,
			Value:       value,
		})
	}

	// add last time series
	if ts != nil {
		res = append(res, ts)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return res, nil
}

func (ch *clickHouse) querySamples(ctx context.Context, start, end int64, fingerprints map[uint64]struct{}) ([]*prompb.TimeSeries, error) {
	// prepare query string
	placeholders := strings.Repeat("?, ", len(fingerprints))
	query := fmt.Sprintf(`
		SELECT fingerprint, timestamp_ms, value
			FROM %s.samples
			WHERE fingerprint IN (%s) AND timestamp_ms >= ? AND timestamp_ms <= ?
			ORDER BY fingerprint, timestamp_ms`,
		ch.database, placeholders[:len(placeholders)-2], // cut last ", "
	)
	query = strings.TrimSpace(query)
	args := make([]interface{}, 0, len(fingerprints)+2)
	for f := range fingerprints {
		args = append(args, f)
	}
	args = append(args, start)
	args = append(args, end)
	ch.l.Debugf("%s %v", query, args)

	// run query
	rows, err := ch.db.Query(query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	return ch.scanSamples(rows)
}

func (ch *clickHouse) tempTableSamples(ctx context.Context, start, end int64, fingerprints map[uint64]struct{}) ([]*prompb.TimeSeries, error) {
	conn, err := ch.db.Conn(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	// create temporary table
	tableName := fmt.Sprintf("promhouse_%d", time.Now().UnixNano())
	query := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			fingerprint UInt64
		)`, tableName)
	query = strings.TrimSpace(query)
	ch.l.Debugf("%s", query)
	if _, err = conn.ExecContext(ctx, query); err != nil {
		return nil, errors.WithStack(err)
	}

	// fill temporary table
	err = inTransaction(ctx, conn, func(tx *sql.Tx) error {
		query = fmt.Sprintf(`INSERT INTO %s (fingerprint) VALUES (?)`, tableName)
		var stmt *sql.Stmt
		var err error
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return errors.WithStack(err)
		}

		for f := range fingerprints {
			// ch.l.Debugf("%s %v", query, f)
			if _, err = stmt.ExecContext(ctx, f); err != nil {
				return errors.WithStack(err)
			}
		}

		return errors.WithStack(stmt.Close())
	})
	if err != nil {
		return nil, err
	}

	// prepare query string
	query = fmt.Sprintf(`
			SELECT fingerprint, timestamp_ms, value
				FROM %s.samples
				ANY INNER JOIN %s USING fingerprint
				WHERE timestamp_ms >= ? AND timestamp_ms <= ?
				ORDER BY fingerprint, timestamp_ms`,
		ch.database, tableName,
	)
	query = strings.TrimSpace(query)
	ch.l.Debugf("%s %v %v", query, start, end)

	// run query
	rows, err := conn.QueryContext(ctx, query, start, end)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	return ch.scanSamples(rows)
}

func (ch *clickHouse) Read(ctx context.Context, queries []base.Query) (*prompb.ReadResponse, error) {
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

	res := &prompb.ReadResponse{
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

		sampleFunc := ch.querySamples
		if len(fingerprints) > ch.maxTimeSeriesInQuery {
			sampleFunc = ch.tempTableSamples
		}
		ts, err := sampleFunc(ctx, int64(q.Start), int64(q.End), fingerprints)
		if err != nil {
			return nil, err
		}
		res.Results[i].TimeSeries = ts
	}

	return res, nil
}

func (ch *clickHouse) Write(ctx context.Context, data *prompb.WriteRequest) error {
	// calculate fingerprints, map them to time series
	fingerprints := make([]uint64, len(data.TimeSeries))
	timeSeries := make(map[uint64][]*prompb.Label, len(data.TimeSeries))
	for i, ts := range data.TimeSeries {
		timeseries.SortLabels(ts.Labels)
		f := timeseries.Fingerprint(ts.Labels)
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
		err := inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
			query := fmt.Sprintf(`INSERT INTO %s.time_series (date, fingerprint, labels) VALUES (?, ?, ?)`, ch.database)
			var stmt *sql.Stmt
			var err error
			if stmt, err = tx.PrepareContext(ctx, query); err != nil {
				return errors.WithStack(err)
			}

			args := make([]interface{}, 3)
			args[0] = model.Now().Time()
			for _, f := range newTimeSeries {
				args[1] = f
				args[2] = marshalLabels(timeSeries[f], make([]byte, 0, 128))
				// ch.l.Debugf("%s %v", query, args)
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
				}
			}

			return errors.WithStack(stmt.Close())
		})
		if err != nil {
			return err
		}
	}

	// write samples
	err := inTransaction(ctx, ch.db, func(tx *sql.Tx) error {
		query := fmt.Sprintf(`INSERT INTO %s.samples (fingerprint, timestamp_ms, value) VALUES (?, ?, ?)`, ch.database)
		var stmt *sql.Stmt
		var err error
		if stmt, err = tx.PrepareContext(ctx, query); err != nil {
			return errors.WithStack(err)
		}

		args := make([]interface{}, 3)
		for i, ts := range data.TimeSeries {
			args[0] = fingerprints[i]

			for _, s := range ts.Samples {
				args[1] = s.TimestampMs
				args[2] = s.Value
				// ch.l.Debugf("%s %v", query, args)
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return errors.WithStack(err)
				}
			}
		}

		return errors.WithStack(stmt.Close())
	})
	if err != nil {
		return err
	}

	n := len(newTimeSeries)
	if n != 0 {
		ch.mWrittenTimeSeries.Add(float64(n))
		ch.l.Debugf("Wrote %d new time series.", n)
	}
	return nil
}

// check interfaces
var (
	_ base.Storage = (*clickHouse)(nil)
)
