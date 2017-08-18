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

	_ "github.com/kshvakov/clickhouse" // register SQL driver
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/util"
)

// ClickHouse implements storage interface for the ClickHouse.
type ClickHouse struct {
	db       *sql.DB
	l        *logrus.Entry
	database string
	// labels   []string

	mSamplesCurrent prometheus.Gauge
	mLabels         *prometheus.CounterVec
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
			Namespace: "promhouse",
			Subsystem: "clickhouse",
			Name:      "samples_current",
			Help:      "Current number of stored samples.",
		}),
		mLabels: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "promhouse",
			Subsystem: "clickhouse",
			Name:      "labels",
			Help:      "Written labels by name.",
		}, []string{"name"}),
	}, nil
}

func (ch *ClickHouse) Describe(c chan<- *prometheus.Desc) {
	ch.mSamplesCurrent.Describe(c)
	ch.mLabels.Describe(c)
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
	ch.mLabels.Collect(c)
}

func (ch *ClickHouse) Read(ctx context.Context, queries []Query) ([]model.Matrix, error) {
	res := make([]model.Matrix, len(queries))
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
				panic("unknown match type")
			}
		}
		query += " ORDER BY __fingerprint__, __ts__"

		rows, err := ch.db.Query(query, args...)
		if err != nil {
			return nil, err
		}

		var ss *model.SampleStream
		var b []byte
		var fingerprint, prevFingerprint uint64
		var ts int64
		var value float64
		for rows.Next() {
			if err = rows.Scan(&b, &fingerprint, &ts, &value); err != nil {
				return nil, err
			}
			if fingerprint != prevFingerprint {
				prevFingerprint = fingerprint
				if ss != nil {
					res[i] = append(res[i], ss)
				}

				var ls model.LabelSet
				if err = ls.UnmarshalJSON(b); err != nil {
					return nil, err
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
			return nil, err
		}
		if ss != nil {
			res[i] = append(res[i], ss)
		}
	}

	return res, nil
}

func (ch *ClickHouse) Write(ctx context.Context, data model.Matrix) (err error) {
	var commited bool
	var tx *sql.Tx
	tx, err = ch.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() {
		if !commited {
			e := tx.Rollback()
			if err == nil {
				err = e
			}
		}
	}()

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
			ch.mLabels.WithLabelValues(string(n)).Inc()
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
	commited = true
	return
}

// check interface
var _ Storage = (*ClickHouse)(nil)
