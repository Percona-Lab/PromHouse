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
	"encoding/json"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/utils/gofuzz"
)

// marshalLabels marshals Prometheus labels into JSON, appending it to b.
// It preserves an order. It is also significantly faster then json.Marshal.
// It is compatible with ClickHouse JSON functions: https://clickhouse.yandex/docs/en/functions/json_functions.html
func marshalLabels(labels []*prompb.Label, b []byte) []byte {
	if len(labels) == 0 {
		return append(b, '{', '}')
	}

	b = append(b, '{')
	for _, l := range labels {
		// add label name which can't contain runes that should be escaped
		b = append(b, '"')
		b = append(b, l.Name...)
		b = append(b, '"', ':', '"')

		// FIXME we don't handle Unicode runes correctly here (first byte >= 0x80)
		// FIXME should we escape \b? \f? What are exact rules?
		// https://github.com/Percona-Lab/PromHouse/issues/19

		// add label value while escaping some runes
		for _, c := range []byte(l.Value) {
			switch c {
			case '\\', '"':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, c)
			}
		}

		b = append(b, '"', ',')
	}
	b[len(b)-1] = '}' // replace last comma

	gofuzz.AddToCorpus("json", b)
	return b
}

// unmarshalLabels unmarshals JSON into Prometheus labels.
// It does not preserves an order.
func unmarshalLabels(b []byte) ([]*prompb.Label, error) {
	m := make(map[string]string)
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	res := make([]*prompb.Label, 0, len(m))
	for n, v := range m {
		res = append(res, &prompb.Label{
			Name:  n,
			Value: v,
		})
	}
	return res, nil
}
