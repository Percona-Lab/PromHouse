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
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	prompb "github.com/Percona-Lab/PromHouse/prompb/prom2"
	"github.com/Percona-Lab/PromHouse/storages"
)

type PromAPI struct {
	Storage storages.Storage
	Logger  *logrus.Entry
}

var snappyPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024)
	},
}

func readPB(req *http.Request, pb proto.Unmarshaler) error {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return err
	}

	dst := snappyPool.Get().([]byte)
	dst = dst[:cap(dst)]
	b, err := snappy.Decode(dst, compressed)
	if err == nil {
		err = pb.Unmarshal(b)
	}
	snappyPool.Put(b)
	return err
}

func (p *PromAPI) convertReadRequest(request *prompb.ReadRequest) []storages.Query {
	queries := make([]storages.Query, len(request.Queries))
	for i, rq := range request.Queries {
		empty := true
		q := storages.Query{
			Start:    model.Time(rq.StartTimestampMs),
			End:      model.Time(rq.EndTimestampMs),
			Matchers: make([]storages.Matcher, len(rq.Matchers)),
		}
		for j, m := range rq.Matchers {
			var t storages.MatchType
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				t = storages.MatchEqual
			case prompb.LabelMatcher_NEQ:
				t = storages.MatchNotEqual
			case prompb.LabelMatcher_RE:
				t = storages.MatchRegexp
			case prompb.LabelMatcher_NRE:
				t = storages.MatchNotRegexp
			default:
				p.Logger.Panicf("expectation failed: unexpected matcher %d", m.Type)
			}

			q.Matchers[j] = storages.Matcher{
				Type:  t,
				Name:  m.Name,
				Value: m.Value,
			}
			if m.Value != "" {
				empty = false
			}
		}

		if empty {
			p.Logger.Panicf("expectation failed: at least one matcher should have non-empty label value")
		}
		queries[i] = q
	}
	return queries
}

func (p *PromAPI) Read(rw http.ResponseWriter, req *http.Request) error {
	var request prompb.ReadRequest
	if err := readPB(req, &request); err != nil {
		return err
	}

	// read from storage
	queries := p.convertReadRequest(&request)
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

func (p *PromAPI) Write(rw http.ResponseWriter, req *http.Request) error {
	var request prompb.WriteRequest
	if err := readPB(req, &request); err != nil {
		return err
	}
	return p.Storage.Write(req.Context(), &request)
}
