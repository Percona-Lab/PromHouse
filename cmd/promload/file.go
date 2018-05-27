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
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// fileClient reads and writes data from/to files in custom format.
type fileClient struct {
	l                    *logrus.Entry
	f                    *os.File
	r                    *bufio.Reader
	w                    *bufio.Writer
	fSize                int64
	bRead, bDecoded      []byte
	bMarshaled, bEncoded []byte
}

func newFileClient(f *os.File) *fileClient {
	var fSize int64
	fi, err := f.Stat()
	if err == nil {
		fSize = fi.Size()
	}
	return &fileClient{
		l:          logrus.WithField("client", fmt.Sprintf("file %s", f.Name())),
		f:          f,
		r:          bufio.NewReader(f),
		w:          bufio.NewWriter(f),
		fSize:      fSize,
		bRead:      make([]byte, 0, 1048576),
		bDecoded:   make([]byte, 0, 1048576),
		bMarshaled: make([]byte, 0, 1048576),
		bEncoded:   make([]byte, 0, 1048576),
	}
}

func (client *fileClient) readTS() tsReadData {
	// read next message reusing bRead
	var err error
	var size uint32
	if err = binary.Read(client.r, binary.BigEndian, &size); err != nil {
		if err == io.EOF {
			return tsReadData{err: err}
		}
		return tsReadData{err: errors.Wrap(err, "failed to read message size")}
	}
	if uint32(cap(client.bRead)) >= size {
		client.bRead = client.bRead[:size]
	} else {
		client.bRead = make([]byte, size)
	}
	if _, err = io.ReadFull(client.r, client.bRead); err != nil {
		return tsReadData{err: errors.Wrap(err, "failed to read message")}
	}

	// decode message reusing bDecoded
	client.bDecoded = client.bDecoded[:cap(client.bDecoded)]
	client.bDecoded, err = snappy.Decode(client.bDecoded, client.bRead)
	if err != nil {
		return tsReadData{err: errors.Wrap(err, "failed to decode message")}
	}

	// unmarshal message
	var ts prompb.TimeSeries
	if err = proto.Unmarshal(client.bDecoded, &ts); err != nil {
		return tsReadData{err: errors.Wrap(err, "failed to unmarshal message")}
	}

	// update progress
	data := tsReadData{
		ts: []*prompb.TimeSeries{&ts},
	}
	if client.fSize != 0 {
		offset, err := client.f.Seek(0, os.SEEK_CUR)
		if err == nil {
			data.current = uint(offset)
			data.max = uint(client.fSize)
		}
	}

	return data
}

func (client *fileClient) runReader(ctx context.Context, ch chan<- tsReadData) {
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

func (client *fileClient) writeTS(ts []*prompb.TimeSeries) error {
	for _, t := range ts {
		// marshal message reusing bMarshaled
		var err error
		size := t.Size()
		if cap(client.bMarshaled) >= size {
			client.bMarshaled = client.bMarshaled[:size]
		} else {
			client.bMarshaled = make([]byte, size)
		}
		size, err = t.MarshalTo(client.bMarshaled)
		if err != nil {
			return errors.Wrap(err, "failed to marshal message")
		}
		if t.Size() != size {
			return errors.Errorf("unexpected size: expected %d, got %d", t.Size(), size)
		}

		// encode message reusing bEncoded
		client.bEncoded = client.bEncoded[:cap(client.bEncoded)]
		client.bEncoded = snappy.Encode(client.bEncoded, client.bMarshaled[:size])

		// write message
		if err = binary.Write(client.w, binary.BigEndian, uint32(len(client.bEncoded))); err != nil {
			return errors.Wrap(err, "failed to write message length")
		}
		if _, err = client.w.Write(client.bEncoded); err != nil {
			return errors.Wrap(err, "failed to write message")
		}
	}
	return nil
}

func (client *fileClient) close() error {
	// always flush, sync, close; return first error
	var err error
	if e := client.w.Flush(); err == nil {
		err = errors.Wrap(e, "failed to flush")
	}
	if e := client.f.Sync(); err == nil {
		err = errors.Wrap(e, "failed to sync")
	}
	if e := client.f.Close(); err == nil {
		err = errors.Wrap(e, "failed to close")
	}
	return err
}

// check interfaces
var (
	_ tsReader = (*fileClient)(nil)
	_ tsWriter = (*fileClient)(nil)
)
