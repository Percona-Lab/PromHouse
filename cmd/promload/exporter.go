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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/utils/timeseries"
)

// exporterClient reads data from Prometheus exporter.
type exporterClient struct {
	l    *logrus.Entry
	url  string
	http *http.Client
	sort bool // set to true for stable results (for example, in tests)

	readParams  *exporterClientReadParams
	readCurrent time.Time
}

type exporterClientReadParams struct {
	start, end time.Time
	step       time.Duration
	cache      bool
}

func (p exporterClientReadParams) String() string {
	return fmt.Sprintf("{start: %s, end: %s, step: %s, cache: %t}", p.start, p.end, p.step, p.cache)
}

func newExporterClient(url string, readParams *exporterClientReadParams) *exporterClient {
	client := &exporterClient{
		l:   logrus.WithField("client", "exporter"),
		url: url,
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     10 * time.Second,
			},
		},
		readParams:  readParams,
		readCurrent: readParams.start,
	}

	return client
}

func (client *exporterClient) getMetrics(ctx context.Context) (io.ReadCloser, http.Header, error) {
	// make request
	req, err := http.NewRequest("GET", client.url, nil)
	if err != nil {
		return nil, nil, err
	}
	req = req.WithContext(ctx)
	resp, err := client.http.Do(req)
	if err != nil {
		return nil, nil, err
	}

	// check response
	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, nil, fmt.Errorf("status code %d: %s", resp.StatusCode, b)
	}

	// return response with open body
	return resp.Body, resp.Header, nil
}

func (client *exporterClient) decodeMetrics(rc io.ReadCloser, headers http.Header) ([]*prompb.TimeSeries, error) {
	// decode metrics
	d := expfmt.NewDecoder(rc, expfmt.ResponseFormat(headers))
	mfs := make([]*dto.MetricFamily, 0, 1000)
	for {
		mf := new(dto.MetricFamily)
		if err := d.Decode(mf); err != nil {
			if err == io.EOF {
				break
			}
			rc.Close()
			return nil, err
		}
		mfs = append(mfs, mf)
	}
	if err := rc.Close(); err != nil {
		return nil, err
	}

	// convert to vector
	vector, err := expfmt.ExtractSamples(new(expfmt.DecodeOptions), mfs...)
	if err != nil {
		return nil, err
	}

	// convert to time series
	ts := make(map[model.Fingerprint]*prompb.TimeSeries, 128)
	for _, s := range vector {
		fp := s.Metric.Fingerprint()
		t := ts[fp]
		if t == nil {
			// make time series
			t = &prompb.TimeSeries{
				Labels:  make([]*prompb.Label, 0, len(s.Metric)),
				Samples: make([]*prompb.Sample, 0, 100),
			}
			for k, v := range s.Metric {
				t.Labels = append(t.Labels, &prompb.Label{
					Name:  string(k),
					Value: string(v),
				})
			}
			if client.sort {
				timeseries.SortLabels(t.Labels)
			}
			ts[fp] = t
		}

		t.Samples = append(t.Samples, &prompb.Sample{
			TimestampMs: int64(s.Timestamp),
			Value:       float64(s.Value),
		})
	}

	res := make([]*prompb.TimeSeries, 0, len(ts))
	for _, t := range ts {
		res = append(res, t)
	}
	if client.sort {
		timeseries.SortTimeSeriesSlow(res)
	}

	return res, nil
}

func (client *exporterClient) runFreshReader(ctx context.Context, ch chan<- tsReadData) {
	var data tsReadData
	var ts []*prompb.TimeSeries
	for {
		rc, headers, err := client.getMetrics(ctx)
		if err == nil {
			ts, err = client.decodeMetrics(rc, headers)
			data = tsReadData{ts: ts, err: err}
		} else {
			data = tsReadData{err: err}
		}

		select {
		case <-ctx.Done():
			data.err = ctx.Err()
			ch <- data
		case ch <- data:
			if data.err == nil {
				continue
			}
		}

		close(ch)
		return
	}
}

func (client *exporterClient) runReader(ctx context.Context, ch chan<- tsReadData) {
	freshCh := make(chan tsReadData)
	go client.runFreshReader(ctx, freshCh)
	data := <-freshCh

	for {
		if client.readCurrent.After(client.readParams.end) {
			ch <- tsReadData{err: io.EOF}
			close(ch)
			return
		}

		start := client.readCurrent
		end := start.Add(client.readParams.step)
		if end.After(client.readParams.end) {
			end = client.readParams.end
		}
		client.readCurrent = end

		for _, t := range data.ts {
			for _, s := range t.Samples {
				s.TimestampMs = int64(model.TimeFromUnixNano(start.UnixNano()))
			}
		}

		var tsCopy []*prompb.TimeSeries
		if client.readParams.cache {
			tsCopy = make([]*prompb.TimeSeries, len(data.ts))
			for i, t := range data.ts {
				tsCopy[i] = proto.Clone(t).(*prompb.TimeSeries)
			}
		}

		data.current = uint(client.readCurrent.Unix() - client.readParams.start.Unix())
		data.max = uint(client.readParams.end.Unix() - client.readParams.start.Unix())
		ch <- data
		if data.err != nil {
			close(ch)
			return
		}

		if client.readParams.cache {
			select {
			case data = <-freshCh:
				client.l.Debug("Got fresh data.")
			default:
				data.ts = tsCopy
				client.l.Debug("Using old data.")
			}
		} else {
			data = <-freshCh
		}
	}
}

func (client *exporterClient) close() error {
	client.http.Transport.(*http.Transport).CloseIdleConnections()
	return nil
}

// check interfaces
var (
	_ tsReader = (*exporterClient)(nil)
)
