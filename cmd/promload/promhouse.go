// Copyright 2017, 2018 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// promHouseClient reads and writes data from/to PromHouse.
// It uses remote API which is expanded version of Prometheus remote API.
type promHouseClient struct {
	l    *logrus.Entry
	url  string
	http *http.Client

	readParams  *promHouseClientReadParams
	readCurrent time.Time

	bMarshaled, bEncoded []byte
	bRead, bDecoded      []byte
}

type promHouseClientReadParams struct {
	start, end time.Time
	step       time.Duration
}

func (p promHouseClientReadParams) String() string {
	return fmt.Sprintf("{start: %s, end: %s, step: %s}", p.start, p.end, p.step)
}

func newPromHouseClient(url string, readParams *promHouseClientReadParams) *promHouseClient {
	client := &promHouseClient{
		l:   logrus.WithField("client", "promhouse"),
		url: url,
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		},

		readParams: readParams,

		bMarshaled: make([]byte, 0, 1048576),
		bEncoded:   make([]byte, 0, 1048576),
		bRead:      make([]byte, 0, 1048576),
		bDecoded:   make([]byte, 0, 1048576),
	}

	if readParams != nil {
		client.readCurrent = readParams.start
	}

	return client
}

func (client *promHouseClient) readTS() tsReadData {
	if client.readCurrent.Equal(client.readParams.end) {
		return tsReadData{err: io.EOF}
	}

	start := client.readCurrent
	end := start.Add(client.readParams.step)
	if end.After(client.readParams.end) {
		end = client.readParams.end
	}
	client.readCurrent = end

	// This request is not valid for Prometheus / PromQL, but valid for PromHouse.
	// See "Empty" test in storages_test.go.
	request := prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: int64(model.TimeFromUnixNano(start.UnixNano())),
			EndTimestampMs:   int64(model.TimeFromUnixNano(end.UnixNano())),
		}},
	}
	client.l.Debugf("Request: %s", request.String())

	// marshal request reusing bMarshaled
	var err error
	size := request.Size()
	if cap(client.bMarshaled) >= size {
		client.bMarshaled = client.bMarshaled[:size]
	} else {
		client.bMarshaled = make([]byte, size)
	}
	size, err = request.MarshalTo(client.bMarshaled)
	if err != nil {
		return tsReadData{err: err}
	}
	if request.Size() != size {
		return tsReadData{err: fmt.Errorf("unexpected size: expected %d, got %d", request.Size(), size)}
	}

	// encode request reusing bEncoded
	client.bEncoded = client.bEncoded[:cap(client.bEncoded)]
	client.bEncoded = snappy.Encode(client.bEncoded, client.bMarshaled[:size])

	req, err := http.NewRequest("POST", client.url, bytes.NewReader(client.bEncoded))
	if err != nil {
		return tsReadData{err: err}
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := client.http.Do(req)
	if err != nil {
		return tsReadData{err: err}
	}
	defer resp.Body.Close()

	// read response reusing bRead
	buf := bytes.NewBuffer(client.bRead[:0])
	if _, err = buf.ReadFrom(resp.Body); err != nil {
		return tsReadData{err: err}
	}
	client.bRead = buf.Bytes()
	if resp.StatusCode != 200 {
		return tsReadData{err: fmt.Errorf("%d: %s", resp.StatusCode, client.bRead)}
	}

	// decode response reusing bDecoded
	client.bDecoded = client.bDecoded[:cap(client.bDecoded)]
	client.bDecoded, err = snappy.Decode(client.bDecoded, client.bRead)
	if err != nil {
		return tsReadData{err: err}
	}

	// unmarshal message
	var response prompb.ReadResponse
	if err = proto.Unmarshal(client.bDecoded, &response); err != nil {
		return tsReadData{err: err}
	}

	return tsReadData{
		ts:      response.Results[0].TimeSeries,
		current: uint(client.readCurrent.Unix() - client.readParams.start.Unix()),
		max:     uint(client.readParams.end.Unix() - client.readParams.start.Unix()),
	}
}

func (client *promHouseClient) runReader(ctx context.Context, ch chan<- tsReadData) {
	for {
		data := client.readTS()
		if data.err == nil {
			data.err = ctx.Err()
		}
		ch <- data
		if data.err != nil {
			close(ch)
			return
		}
	}
}

func (client *promHouseClient) writeTS(ts []*prompb.TimeSeries) error {
	request := prompb.WriteRequest{
		TimeSeries: ts,
	}
	client.l.Debugf("Request: %s", request.String())

	// marshal request reusing bMarshaled
	var err error
	size := request.Size()
	if cap(client.bMarshaled) >= size {
		client.bMarshaled = client.bMarshaled[:size]
	} else {
		client.bMarshaled = make([]byte, size)
	}
	size, err = request.MarshalTo(client.bMarshaled)
	if err != nil {
		return err
	}
	if request.Size() != size {
		return fmt.Errorf("unexpected size: expected %d, got %d", request.Size(), size)
	}

	// encode request reusing bEncoded
	client.bEncoded = client.bEncoded[:cap(client.bEncoded)]
	client.bEncoded = snappy.Encode(client.bEncoded, client.bMarshaled[:size])

	req, err := http.NewRequest("POST", client.url, bytes.NewReader(client.bEncoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := client.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("%d: %s", resp.StatusCode, b)
	}
	return nil
}

func (client *promHouseClient) close() error {
	client.http.Transport.(*http.Transport).CloseIdleConnections()
	return nil
}

// check interfaces
var (
	_ tsReader = (*promHouseClient)(nil)
	_ tsWriter = (*promHouseClient)(nil)
)
