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
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/storages"
)

func getWriteRequest() *prompb.WriteRequest {
	start := model.Now().Add(-6 * time.Second)

	return &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "query"},
				},
				Samples: []*prompb.Sample{
					{Value: 13, Timestamp: int64(start)},
					{Value: 14, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 14, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 14, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 15, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "400"},
					{Name: "handler", Value: "query_range"},
				},
				Samples: []*prompb.Sample{
					{Value: 9, Timestamp: int64(start)},
					{Value: 9, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 9, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 11, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 11, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
			{
				Labels: []*prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "code", Value: "200"},
					{Name: "handler", Value: "prometheus"},
				},
				Samples: []*prompb.Sample{
					{Value: 591, Timestamp: int64(start)},
					{Value: 592, Timestamp: int64(start.Add(1 * time.Second))},
					{Value: 593, Timestamp: int64(start.Add(2 * time.Second))},
					{Value: 594, Timestamp: int64(start.Add(3 * time.Second))},
					{Value: 595, Timestamp: int64(start.Add(4 * time.Second))},
				},
			},
		},
	}
}

func TestWrite(t *testing.T) {
	h := PromAPI{
		Storage: new(storages.Blackhole),
		Logger: logrus.NewEntry(&logrus.Logger{
			Level: logrus.FatalLevel,
		}),
	}

	data, err := proto.Marshal(getWriteRequest())
	require.NoError(t, err)
	r := bytes.NewReader(snappy.Encode(nil, data))
	req, err := http.NewRequest("", "", r)
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

	data, err := proto.Marshal(getWriteRequest())
	require.NoError(b, err)
	r := bytes.NewReader(snappy.Encode(nil, data))
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
