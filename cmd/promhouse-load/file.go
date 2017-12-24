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
	"encoding/binary"
	"io"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/Percona-Lab/PromHouse/prompb"
)

type fileClient struct {
	f                    *os.File
	bRead, bDecoded      []byte
	bMarshaled, bEncoded []byte
}

func newFileClient(f *os.File) *fileClient {
	return &fileClient{
		f:          f,
		bRead:      make([]byte, 1048576),
		bDecoded:   make([]byte, 1048576),
		bMarshaled: make([]byte, 1048576),
		bEncoded:   make([]byte, 1048576),
	}
}

func (fc *fileClient) readTS() (*prompb.TimeSeries, error) {
	// read next message reusing bRead
	var err error
	var size uint32
	if err = binary.Read(fc.f, binary.BigEndian, size); err != nil {
		return nil, errors.Wrap(err, "failed to read message size")
	}
	if uint32(cap(fc.bRead)) >= size {
		fc.bRead = fc.bRead[:size]
	} else {
		fc.bRead = make([]byte, size)
	}
	if _, err = io.ReadFull(fc.f, fc.bRead); err != nil {
		return nil, errors.Wrap(err, "failed to read message")
	}

	// decode message reusing bDecoded
	fc.bDecoded = fc.bDecoded[:cap(fc.bDecoded)]
	fc.bDecoded, err = snappy.Decode(fc.bDecoded, fc.bRead)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode message")
	}

	// unmarshal message
	var ts prompb.TimeSeries
	if err = proto.Unmarshal(fc.bDecoded, &ts); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal message")
	}
	return &ts, nil
}

func (fc *fileClient) writeTS(ts *prompb.TimeSeries) error {
	// marshal message reusing bMarshaled
	var err error
	size := ts.Size()
	if cap(fc.bMarshaled) >= size {
		fc.bMarshaled = fc.bMarshaled[:size]
	} else {
		fc.bMarshaled = make([]byte, size)
	}
	size, err = ts.MarshalTo(fc.bMarshaled)
	if err != nil {
		return errors.Wrap(err, "failed to marshal message")
	}

	// encode message reusing bEncoded
	fc.bEncoded = fc.bEncoded[:cap(fc.bEncoded)]
	fc.bEncoded = snappy.Encode(fc.bEncoded, fc.bMarshaled[:size])

	// write message
	if err = binary.Write(fc.f, binary.BigEndian, uint32(len(fc.bEncoded))); err != nil {
		return errors.Wrap(err, "failed to write message length")
	}
	if _, err = fc.f.Write(fc.bEncoded); err != nil {
		return errors.Wrap(err, "failed to write message")
	}
	return nil
}

// check interfaces
var (
	_ tsReader = (*fileClient)(nil)
	_ tsWriter = (*fileClient)(nil)
)
