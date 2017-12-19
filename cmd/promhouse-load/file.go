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

	"github.com/Percona-Lab/PromHouse/prompb"
)

type fileClient struct {
	f *os.File
}

func (fc *fileClient) read() (*prompb.TimeSeries, error) {
	var size uint32
	if err := binary.Read(fc.f, binary.BigEndian, size); err != nil {
		return nil, err
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(fc.f, b); err != nil {
		return nil, err
	}

	b, err := snappy.Decode(nil, b)
	if err != nil {
		return nil, err
	}
	var ts prompb.TimeSeries
	if err = proto.Unmarshal(b, &ts); err != nil {
		return nil, err
	}
	return &ts, nil
}

func (fc *fileClient) write(ts *prompb.TimeSeries) error {
	b, err := proto.Marshal(ts)
	if err != nil {
		return err
	}
	b = snappy.Encode(nil, b)

	size := uint32(len(b))
	if err = binary.Write(fc.f, binary.BigEndian, size); err != nil {
		return err
	}
	_, err = fc.f.Write(b)
	return err
}
