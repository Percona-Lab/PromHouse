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

	prom2 "github.com/Percona-Lab/PromHouse/prompb/prom2"
	"github.com/Percona-Lab/PromHouse/storages"
)

func (p *PromAPI) convertRead2Request(request *prom2.ReadRequest) []storages.Query {
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
			case prom2.LabelMatcher_EQ:
				t = storages.MatchEqual
			case prom2.LabelMatcher_NEQ:
				t = storages.MatchNotEqual
			case prom2.LabelMatcher_RE:
				t = storages.MatchRegexp
			case prom2.LabelMatcher_NRE:
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

func (p *PromAPI) Read2(rw http.ResponseWriter, req *http.Request) error {
	var request prom2.ReadRequest
	if err := readPB(req, &request); err != nil {
		return err
	}

	// read from storage
	queries := p.convertRead2Request(&request)
	p.Logger.Infof("Queries: %s", queries)
	response, err := p.Storage.Read(req.Context(), queries)
	if err != nil {
		return err
	}
	p.Logger.Debugf("Response data:\n%s", response)

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

func (p *PromAPI) Write2(rw http.ResponseWriter, req *http.Request) error {
	var request prom2.WriteRequest
	if err := readPB(req, &request); err != nil {
		return err
	}
	return p.Storage.Write(req.Context(), &request)
}
