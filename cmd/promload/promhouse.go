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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
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
	http *http.Client
	url  string

	readParams  *promHouseClientReadParams
	readCurrent time.Time

	bMarshaled, bEncoded []byte
	bRead, bDecoded      []byte

	toWrite  chan []*prompb.TimeSeries
	writerWG sync.WaitGroup
}

type promHouseClientReadParams struct {
	start, end time.Time
	step       time.Duration
}

func newPromHouseClient(url string, readParams *promHouseClientReadParams) *promHouseClient {
	client := &promHouseClient{
		l: logrus.WithField("client", "promhouse"),
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		},
		url: url,

		readParams: readParams,

		bMarshaled: make([]byte, 0, 1048576),
		bEncoded:   make([]byte, 0, 1048576),
		bRead:      make([]byte, 0, 1048576),
		bDecoded:   make([]byte, 0, 1048576),

		toWrite: make(chan []*prompb.TimeSeries, 10000),
	}

	if readParams != nil {
		client.readCurrent = readParams.start
	}

	client.writerWG.Add(1)
	go client.runWriter()
	return client
}

func (client *promHouseClient) readTS() ([]*prompb.TimeSeries, *readProgress, error) {
	if client.readCurrent.Equal(client.readParams.end) {
		return nil, nil, io.EOF
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
	client.l.Debugf("Request: %s", request)

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
		return nil, nil, err
	}
	if request.Size() != size {
		return nil, nil, fmt.Errorf("unexpected size: expected %d, got %d", request.Size(), size)
	}

	// encode request reusing bEncoded
	client.bEncoded = client.bEncoded[:cap(client.bEncoded)]
	client.bEncoded = snappy.Encode(client.bEncoded, client.bMarshaled[:size])

	req, err := http.NewRequest("POST", client.url, bytes.NewReader(client.bEncoded))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := client.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	// read response reusing bRead
	buf := bytes.NewBuffer(client.bRead[:0])
	if _, err = buf.ReadFrom(resp.Body); err != nil {
		return nil, nil, err
	}
	client.bRead = buf.Bytes()
	if resp.StatusCode != 200 {
		return nil, nil, fmt.Errorf("%d: %s", resp.StatusCode, client.bRead)
	}

	// decode response reusing bDecoded
	client.bDecoded = client.bDecoded[:cap(client.bDecoded)]
	client.bDecoded, err = snappy.Decode(client.bDecoded, client.bRead)
	if err != nil {
		return nil, nil, err
	}

	// unmarshal message
	var response prompb.ReadResponse
	if err = proto.Unmarshal(client.bDecoded, &response); err != nil {
		return nil, nil, err
	}

	rp := &readProgress{
		current: uint(client.readCurrent.Unix() - client.readParams.start.Unix()),
		max:     uint(client.readParams.end.Unix() - client.readParams.start.Unix()),
	}
	return response.Results[0].TimeSeries, rp, nil
}

func (client *promHouseClient) write(ts []*prompb.TimeSeries) error {
	request := prompb.WriteRequest{
		TimeSeries: ts,
	}
	client.l.Debugf("Request: %s", request)

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

func (client *promHouseClient) runWriter() {
	// drain channel
	defer func() {
		for range client.toWrite {
		}
		client.writerWG.Done()
	}()

	const maxBatch = 66
	ts := make([]*prompb.TimeSeries, 0, maxBatch)
	for {
		var t []*prompb.TimeSeries
		for len(ts) < maxBatch {
			t = <-client.toWrite
			if t == nil {
				break
			}
			ts = append(ts, t...)
		}
		_ = client.write(ts)
	}
}

func (client *promHouseClient) writeTS(ts []*prompb.TimeSeries) error {
	client.toWrite <- ts
	return nil
}

func (client *promHouseClient) close() error {
	close(client.toWrite)
	client.writerWG.Wait()
	client.http.Transport.(*http.Transport).CloseIdleConnections()
	return nil
}

// check interfaces
var (
	_ tsReader = (*promHouseClient)(nil)
	_ tsWriter = (*promHouseClient)(nil)
)
