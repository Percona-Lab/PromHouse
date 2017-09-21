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

package handlers

import (
	"net/http"

	prom1 "github.com/Percona-Lab/PromHouse/prompb/prom1"
	prom2 "github.com/Percona-Lab/PromHouse/prompb/prom2"
)

func (p *PromAPI) Read1(rw http.ResponseWriter, req *http.Request) error {
	return nil
}

func (p *PromAPI) Write1(rw http.ResponseWriter, req *http.Request) error {
	var request1 prom1.WriteRequest
	if err := readPB(req, &request1); err != nil {
		return err
	}

	request2 := prom2.WriteRequest{
		Timeseries: make([]*prom2.TimeSeries, len(request1.Timeseries)),
	}
	for i, ts1 := range request1.Timeseries {
		ts2 := prom2.TimeSeries{
			Labels:  make([]*prom2.Label, len(ts1.Labels)),
			Samples: make([]*prom2.Sample, len(ts1.Samples)),
		}
		for j, lp := range ts1.Labels {
			ts2.Labels[j] = &prom2.Label{
				Name:  lp.Name,
				Value: lp.Value,
			}
		}
		for j, s := range ts1.Samples {
			ts2.Samples[j] = &prom2.Sample{
				Value:     s.Value,
				Timestamp: s.TimestampMs,
			}
		}
		request2.Timeseries[i] = &ts2
	}
	return p.Storage.Write(req.Context(), &request2)
}
