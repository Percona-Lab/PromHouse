// promhouse
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
	"io/ioutil"
	"net/http"
	"net/url"
	"path"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

type promhouseClient struct {
	l                    *logrus.Entry
	client               *http.Client
	writeURL             string
	bMarshaled, bEncoded []byte
}

func newpromhouseClient(base string) (*promhouseClient, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	u.Path = path.Join(u.Path, "/write")

	pc := &promhouseClient{
		l:          logrus.WithField("client", "promhouse"),
		client:     new(http.Client),
		writeURL:   u.String(),
		bMarshaled: make([]byte, 1048576),
		bEncoded:   make([]byte, 1048576),
	}
	return pc, nil
}

func (pc *promhouseClient) writeTS(ts *prompb.TimeSeries) error {
	request := prompb.WriteRequest{
		TimeSeries: []*prompb.TimeSeries{ts},
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
		return errors.Wrap(err, "failed to marshal request")
	}
	if request.Size() != size {
		return errors.Errorf("unexpected size: expected %d, got %d", request.Size(), size)
	}

	// encode request reusing bEncoded
	pc.bEncoded = pc.bEncoded[:cap(pc.bEncoded)]
	pc.bEncoded = snappy.Encode(pc.bEncoded, pc.bMarshaled[:size])

	req, err := http.NewRequest("POST", pc.writeURL, bytes.NewReader(pc.bEncoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	resp, err := pc.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf("unexpected response code %d: %s", resp.StatusCode, b)
	}
	return nil
}

// check interface
var _ tsWriter = (*promhouseClient)(nil)
