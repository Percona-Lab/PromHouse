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
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// tsdbClient reads and writes data from/to Prometheus TSDB.
type tsdbClient struct {
	l *logrus.Entry
}

func newTSDBClient() *tsdbClient {
	return &tsdbClient{
		l: logrus.WithField("client", fmt.Sprintf("tsdb")),
	}
}

func (client *tsdbClient) readTS() ([]*prompb.TimeSeries, *readProgress, error) {
	panic("not implemented")
}

func (client *tsdbClient) writeTS(ts []*prompb.TimeSeries) error {
	panic("not implemented")
}

func (client *tsdbClient) close() error {
	panic("not implemented")
}

// check interfaces
var (
	_ tsReader = (*tsdbClient)(nil)
	_ tsWriter = (*tsdbClient)(nil)
)
