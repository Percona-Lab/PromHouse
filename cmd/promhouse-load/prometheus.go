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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

type prometheusClient struct {
	l                    *logrus.Entry
	client               *http.Client
	readURL              string
	bRead, bDecoded      []byte
	bMarshaled, bEncoded []byte
	start                time.Time
	end                  time.Time
	step                 time.Duration
	currentTime          time.Time
	ts                   []map[string]string
	currentTS            int
}

func newPrometheusClient(base string, start, end time.Time, step time.Duration, maxTS int) (*prometheusClient, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	basePath := u.Path

	u.Path = path.Join(basePath, "/api/v1/read")
	pc := &prometheusClient{
		l:           logrus.WithField("client", "prometheus"),
		client:      new(http.Client),
		readURL:     u.String(),
		bRead:       make([]byte, 1048576),
		bDecoded:    make([]byte, 1048576),
		bMarshaled:  make([]byte, 1024),
		bEncoded:    make([]byte, 1024),
		start:       start,
		end:         end,
		step:        step,
		currentTime: start,
	}

	u.Path = path.Join(basePath, "/api/v1/series")
	v := make(url.Values)
	v.Set(`match[]`, `{__name__!=""}`)
	v.Set(`start`, strconv.FormatInt(start.Unix(), 10))
	v.Set(`end`, strconv.FormatInt(end.Unix(), 10))
	u.RawQuery = v.Encode()

	resp, err := pc.client.Get(u.String())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf("unexpected response code %d: %s", resp.StatusCode, b)
	}

	var res struct {
		Data []map[string]string
	}
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, errors.WithStack(err)
	}
	pc.ts = res.Data
	pc.l.Infof("Got %d time series.", len(pc.ts))
	if maxTS > 0 {
		pc.ts = pc.ts[:maxTS]
	}

	pc.l.Infof("Reading %d time series between %s and %s in %s intervals.", len(pc.ts), pc.start, pc.end, pc.step)
	return pc, nil
}

func (pc *prometheusClient) readTS() (*prompb.TimeSeries, error) {
	if pc.currentTime.Equal(pc.end) {
		pc.currentTime = pc.start
		pc.currentTS++
		pc.l.Infof("Read %d time series out of %d.", pc.currentTS, len(pc.ts))
		if pc.currentTS == len(pc.ts) {
			return nil, io.EOF
		}
	}

	ts := pc.ts[pc.currentTS]
	matchers := make([]*prompb.LabelMatcher, 0, len(ts))
	for n, v := range ts {
		matchers = append(matchers, &prompb.LabelMatcher{
			Type:  prompb.LabelMatcher_EQ,
			Name:  n,
			Value: v,
		})
	}

	start := pc.currentTime
	end := start.Add(pc.step)
	if end.After(pc.end) {
		end = pc.end
	}
	pc.currentTime = end
	request := prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: int64(model.TimeFromUnixNano(start.UnixNano())),
			EndTimestampMs:   int64(model.TimeFromUnixNano(end.UnixNano())),
			Matchers:         matchers,
		}},
	}
	pc.l.Debugf("Request: %s", request)

	// marshal request reusing bMarshaled
	var err error
	size := request.Size()
	if cap(pc.bMarshaled) >= size {
		pc.bMarshaled = pc.bMarshaled[:size]
	} else {
		pc.bMarshaled = make([]byte, size)
	}
	size, err = request.MarshalTo(pc.bMarshaled)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request")
	}

	// encode request reusing bEncoded
	pc.bEncoded = pc.bEncoded[:cap(pc.bEncoded)]
	pc.bEncoded = snappy.Encode(pc.bEncoded, pc.bMarshaled[:size])

	req, err := http.NewRequest("POST", pc.readURL, bytes.NewReader(pc.bEncoded))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := pc.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// read response reusing bRead
	buf := bytes.NewBuffer(pc.bRead[:0])
	if _, err = buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	pc.bRead = buf.Bytes()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected response code %d: %s", resp.StatusCode, pc.bRead)
	}

	// decode response reusing bDecoded
	pc.bDecoded = pc.bDecoded[:cap(pc.bDecoded)]
	pc.bDecoded, err = snappy.Decode(pc.bDecoded, pc.bRead)
	if err != nil {
		return nil, err
	}

	// unmarshal message
	var response prompb.ReadResponse
	if err = proto.Unmarshal(pc.bDecoded, &response); err != nil {
		return nil, err
	}
	t := response.Results[0].TimeSeries
	if len(t) != 1 {
		return nil, fmt.Errorf("expected 1 time series, got %d", len(t))
	}
	pc.l.Debugf("Got %s with %d samples.", t[0].Labels, len(t[0].Samples))
	return t[0], nil
}

// check interface
var _ tsReader = (*prometheusClient)(nil)
