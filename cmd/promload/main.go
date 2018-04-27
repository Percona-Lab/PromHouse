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
	"io"
	"log"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/utils/duration"
)

type tsReader interface {
	readTS() (*prompb.TimeSeries, error)
}

type tsWriter interface {
	writeTS(ts *prompb.TimeSeries) error
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		fromArg = kingpin.Arg("from", "Read data from").Required().URL()
		toArg   = kingpin.Arg("to", "Write data to").Required().URL()

		lastF = duration.FromFlag(kingpin.Flag("from-last", "Remote API: read from that time ago").Default("30d"))
		stepF = duration.FromFlag(kingpin.Flag("from-step", "Remote API: interval for a single request").Default("1m"))

		debugF = kingpin.Flag("debug", "Enable debug output").Bool()
	)
	kingpin.Parse()

	logrus.SetLevel(logrus.InfoLevel)
	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var reader tsReader
	switch {
	case (*fromArg).Scheme == "":
		f, err := os.Open((*fromArg).String())
		if err != nil {
			logrus.Fatal(err)
		}
		defer func() {
			if err = f.Close(); err != nil {
				logrus.Error(err)
			}
			logrus.Infof("%s closed.", f.Name())
		}()

		reader = newFileClient(f)

	default:
		end := time.Now().Truncate(time.Minute)
		start := end.Add(-time.Duration(*lastF))
		logrus.Infof("Reading metrics between %s and %s with step %s.", start, end, *stepF)
		reader = newRemoteClient((*fromArg).String(), start, end, time.Duration(*stepF))
	}

	var writer tsWriter
	switch {
	case (*toArg).Scheme == "":
		f, err := os.Create((*toArg).String())
		if err != nil {
			logrus.Fatal(err)
		}
		defer func() {
			if err = f.Close(); err != nil {
				logrus.Error(err)
			}
			logrus.Infof("%s closed.", f.Name())
		}()

		writer = newFileClient(f)

	default:
		writer = newRemoteClient((*toArg).String(), time.Time{}, time.Time{}, 0)
	}

	ch := make(chan *prompb.TimeSeries, 100)
	go func() {
		for {
			ts, err := reader.readTS()
			if err != nil {
				if err != io.EOF {
					logrus.Errorf("Read error: %+v", err)
				}
				close(ch)
				return
			}
			if ts != nil {
				ch <- ts
			}
		}
	}()

	for ts := range ch {
		if err := writer.writeTS(ts); err != nil {
			logrus.Errorf("Write error: %+v", err)
			return
		}
	}
}
