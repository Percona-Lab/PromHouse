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

	"github.com/golang/snappy"
	"github.com/prometheus/common/model"

	prom1 "github.com/Percona-Lab/PromHouse/prompb/prom1"
	prom2 "github.com/Percona-Lab/PromHouse/prompb/prom2"
	"github.com/Percona-Lab/PromHouse/storages"
)

func (p *PromAPI) convertRead1Request(request *prom1.ReadRequest) []storages.Query {
	queries := make([]storages.Query, len(request.Queries))
	for i, rq := range request.Queries {
		q := storages.Query{
			Start:    model.Time(rq.StartTimestampMs),
			End:      model.Time(rq.EndTimestampMs),
			Matchers: make([]storages.Matcher, len(rq.Matchers)),
		}

		for j, m := range rq.Matchers {
			var t storages.MatchType
			switch m.Type {
			case prom1.MatchType_EQUAL:
				t = storages.MatchEqual
			case prom1.MatchType_NOT_EQUAL:
				t = storages.MatchNotEqual
			case prom1.MatchType_REGEX_MATCH:
				t = storages.MatchRegexp
			case prom1.MatchType_REGEX_NO_MATCH:
				t = storages.MatchNotRegexp
			default:
				p.Logger.Panicf("expectation failed: unexpected matcher %d", m.Type)
			}

			q.Matchers[j] = storages.Matcher{
				Type:  t,
				Name:  m.Name,
				Value: m.Value,
			}
		}

		queries[i] = q
	}

	return queries
}

func (p *PromAPI) Read1(rw http.ResponseWriter, req *http.Request) error {
	var request prom1.ReadRequest
	if err := readPB(req, &request); err != nil {
		return err
	}

	// read from storage
	queries := p.convertRead1Request(&request)
	p.Logger.Infof("Queries: %s", queries)
	response, err := p.Storage.Read(req.Context(), queries)
	if err != nil {
		return err
	}
	p.Logger.Debugf("Response data:\n%s", response)

	// TODO convert to prom1.ReadResponse

	// marshal, encode and write response
	// TODO use MarshalTo with sync.Pool?
	b, err := response.Marshal()
	if err != nil {
		return err
	}
	rw.Header().Set("Content-Type", "application/x-protobuf")
	rw.Header().Set("Content-Encoding", "snappy")
	dst := snappyPool.Get().([]byte)
	dst = dst[:cap(dst)]
	compressed := snappy.Encode(dst, b)
	_, err = rw.Write(compressed)
	snappyPool.Put(compressed)
	return err
}

func (p *PromAPI) Write1(rw http.ResponseWriter, req *http.Request) error {
	var request1 prom1.WriteRequest
	if err := readPB(req, &request1); err != nil {
		return err
	}

	// TODO optimize. use unsafe? use gogoproto options?
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
