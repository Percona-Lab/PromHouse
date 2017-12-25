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
	"flag"
	"io"
	"log"
	"os"
	"time"

	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"

	"github.com/Percona-Lab/PromHouse/prompb"
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

	lastF := model.Duration(30 * 24 * time.Hour)
	flag.Var(&lastF, "read-prometheus-last", "Read from Prometheus since that time ago")
	stepF := model.Duration(3 * time.Hour)
	flag.Var(&stepF, "read-prometheus-step", "Interval for a single request to Prometheus")
	var (
		debugF               = flag.Bool("debug", false, "Enable debug outout")
		readFileF            = flag.String("read-file", "", "Read from a given file")
		readPrometheusF      = flag.String("read-prometheus", "", "Read from a given Prometheus")
		readPrometheusMaxTSF = flag.Int("read-prometheus-max-ts", 0, "Maximum number of time series to read from Prometheus")
		writeFileF           = flag.String("write-file", "", "Write to a given file")
		writePromHouseF      = flag.String("write-promhouse", "", "Write to a given PromHouse")
	)
	flag.Parse()

	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}

	var reader tsReader
	switch {
	case *readFileF != "":
		f, err := os.Open(*readFileF)
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

	case *readPrometheusF != "":
		end := time.Now().Truncate(time.Minute)
		start := end.Add(-time.Duration(lastF))
		var err error
		reader, err = newPrometheusClient(*readPrometheusF, start, end, time.Duration(stepF), *readPrometheusMaxTSF)
		if err != nil {
			logrus.Fatal(err)
		}

	default:
		logrus.Fatal("No -read-* flag given.")
	}

	var writer tsWriter
	switch {
	case *writeFileF != "":
		f, err := os.Create(*writeFileF)
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

	case *writePromHouseF != "":
		var err error
		writer, err = newpromhouseClient(*writePromHouseF)
		if err != nil {
			logrus.Fatal(err)
		}

	default:
		logrus.Fatal("No -write-* flag given.")
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
