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
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/storages"
)

func getData(t require.TestingT) *bytes.Reader {
	start := int64(model.TimeFromUnixNano(time.Now().UnixNano()))
	request := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "query"},
				},
				Samples: []*prompb.Sample{
					{Value: 13, Timestamp: start},
					{Value: 14, Timestamp: start + 1},
					{Value: 14, Timestamp: start + 2},
					{Value: 14, Timestamp: start + 3},
					{Value: 15, Timestamp: start + 4},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "400"},
					{Name: "handler", Value: "query_range"},
				},
				Samples: []*prompb.Sample{
					{Value: 9, Timestamp: start},
					{Value: 9, Timestamp: start + 1},
					{Value: 9, Timestamp: start + 2},
					{Value: 11, Timestamp: start + 3},
					{Value: 11, Timestamp: start + 4},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "prometheus"},
				},
				Samples: []*prompb.Sample{
					{Value: 591, Timestamp: start},
					{Value: 592, Timestamp: start + 1},
					{Value: 593, Timestamp: start + 2},
					{Value: 594, Timestamp: start + 3},
					{Value: 595, Timestamp: start + 4},
				},
			},
		},
	}
	b, err := proto.Marshal(request)
	require.NoError(t, err)
	return bytes.NewReader(snappy.Encode(nil, b))
}

func TestWrite(t *testing.T) {
	h := PromAPI{
		Storage: new(storages.Blackhole),
		Logger: logrus.NewEntry(&logrus.Logger{
			Level: logrus.FatalLevel,
		}),
	}

	req, err := http.NewRequest("", "", getData(t))
	require.NoError(t, err)
	require.NoError(t, h.Write(nil, req))
}

func BenchmarkWrite(b *testing.B) {
	h := PromAPI{
		Storage: new(storages.Blackhole),
		Logger: logrus.NewEntry(&logrus.Logger{
			Level: logrus.FatalLevel,
		}),
	}

	r := getData(b)
	req, err := http.NewRequest("", "", r)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Seek(0, io.SeekStart)
		err = h.Write(nil, req)
	}
	b.StopTimer()

	require.NoError(b, err)
}
