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

	"github.com/Percona-Lab/PromHouse/prompb"
)

type scanner struct {
	f float64
	s string
}

func (s *scanner) Scan(v interface{}) error {
	s.f = 0
	s.s = ""

	s.s = fmt.Sprintf("%v", v)
	switch v := v.(type) {
	case int64:
		s.f = float64(v)
	case uint64:
		s.f = float64(v)
	case float64:
		s.f = v
	case []byte:
		s.s = fmt.Sprintf("%s", v)
	}
	return nil
}

func (ch *clickHouse) readRawSQL(ctx context.Context, query string, ts int64) (*prompb.ReadResponse, error) {
	rows, err := ch.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var res prompb.QueryResult
	targets := make([]interface{}, len(columns))
	for i := range targets {
		targets[i] = new(scanner)
	}

	for rows.Next() {
		if err = rows.Scan(targets...); err != nil {
			return nil, err
		}

		labels := make([]*prompb.Label, 0, len(columns))
		var value float64
		for i, c := range columns {
			v := targets[i].(*scanner)
			switch c {
			case "value":
				value = v.f
			default:
				labels = append(labels, &prompb.Label{
					Name:  c,
					Value: v.s,
				})
			}
		}

		res.TimeSeries = append(res.TimeSeries, &prompb.TimeSeries{
			Labels: labels,
			Samples: []*prompb.Sample{{
				Value:       value,
				TimestampMs: ts,
			}},
		})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &prompb.ReadResponse{
		Results: []*prompb.QueryResult{&res},
	}, nil
}

// check interfaces
var (
	_ sql.Scanner = (*scanner)(nil)
)
