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

package timeseries

import (
	"sort"

	"github.com/Percona-Lab/PromHouse/prompb"
)

const nameLabel string = "__name__"

// SortLabels sorts prompb labels by name.
func SortLabels(labels []*prompb.Label) {
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
}

// SortTimeSeriesSlow sorts time series by metric name and fingerprint.
// It is slow, and should be used only in tests.
func SortTimeSeriesSlow(timeSeries []*prompb.TimeSeries) {
	sort.Slice(timeSeries, func(i, j int) bool {
		SortLabels(timeSeries[i].Labels)
		SortLabels(timeSeries[j].Labels)

		var nameI, nameJ string
		for _, l := range timeSeries[i].Labels {
			if l.Name == nameLabel {
				nameI = l.Value
				break
			}
		}
		for _, l := range timeSeries[j].Labels {
			if l.Name == nameLabel {
				nameJ = l.Value
				break
			}
		}
		if nameI != nameJ {
			return nameI < nameJ
		}

		return Fingerprint(timeSeries[i].Labels) < Fingerprint(timeSeries[j].Labels)
	})
}
