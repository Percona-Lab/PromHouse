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

package util

import (
	"github.com/prometheus/common/model"
)

// MarshalMetric marshals Prometheus metric into JSON.
// It is significantly faster then json.Marshal.
// It is compatible with ClickHouse JSON functions: https://clickhouse.yandex/docs/en/functions/json_functions.html
func MarshalMetric(m model.Metric) []byte {
	b := make([]byte, 0, 128)
	b = append(b, '{')
	for k, v := range m {
		// add label name which can't contain runes that should be escaped
		b = append(b, '"')
		b = append(b, k...)
		b = append(b, '"', ':', '"')

		// add label value while escaping some runes
		for _, c := range []byte(v) {
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

	b[len(b)-1] = '}'
	return b
}
