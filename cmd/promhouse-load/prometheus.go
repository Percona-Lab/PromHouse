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
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"

	"github.com/Percona-Lab/PromHouse/prompb"
)

type prometheusClient struct {
	base   string
	client *http.Client
}

// requestTimeSeries requests all time series (label sets, without samples) from Prometheus via API.
func (pc *prometheusClient) requestTimeSeries(start, end time.Time) ([]map[string]string, error) {
	u, err := url.Parse(pc.base)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "/api/v1/series")
	v := make(url.Values)
	v.Set(`match[]`, `{__name__!=""}`)
	v.Set(`start`, strconv.FormatInt(start.Unix(), 10))
	v.Set(`end`, strconv.FormatInt(end.Unix(), 10))
	u.RawQuery = v.Encode()

	resp, err := pc.client.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected response code %d: %s", resp.StatusCode, b)
	}

	var res struct {
		Data []map[string]string
	}
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// requestMetrics requests all samples for a given time series from Prometheus via API.
func (pc *prometheusClient) requestMetrics(start, end time.Time, ts map[string]string) (*prompb.TimeSeries, error) {
	u, err := url.Parse(pc.base)
	if err != nil {
		return nil, err
	}
	u.Path = path.Join(u.Path, "/api/v1/read")

	matchers := make([]*prompb.LabelMatcher, 0, len(ts))
	for n, v := range ts {
		matchers = append(matchers, &prompb.LabelMatcher{
			Type:  prompb.LabelMatcher_EQ,
			Name:  n,
			Value: v,
		})
	}
	request := prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: int64(model.TimeFromUnixNano(start.UnixNano())),
			EndTimestampMs:   int64(model.TimeFromUnixNano(end.UnixNano())),
			Matchers:         matchers,
		}},
	}

	b, err := proto.Marshal(&request)
	if err != nil {
		return nil, err
	}
	b = snappy.Encode(nil, b)
	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(b))
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
	if b, err = ioutil.ReadAll(resp.Body); err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected response code %d: %s", resp.StatusCode, b)
	}

	if b, err = snappy.Decode(nil, b); err != nil {
		return nil, err
	}
	var response prompb.ReadResponse
	if err = proto.Unmarshal(b, &response); err != nil {
		return nil, err
	}
	t := response.Results[0].TimeSeries
	if len(t) != 1 {
		return nil, fmt.Errorf("expected 1 time series, got %d", len(t))
	}
	return t[0], nil
}
