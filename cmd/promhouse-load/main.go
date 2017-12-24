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

	lastF := model.Duration(30 * 24 * time.Hour)
	flag.Var(&lastF, "read-prometheus-last", "Read from Prometheus since that time ago")
	stepF := model.Duration(3 * time.Hour)
	flag.Var(&stepF, "read-prometheus-step", "Interval for a single request to Prometheus")
	var (
		debugF          = flag.Bool("debug", false, "Enable debug outout")
		readPrometheusF = flag.String("read-prometheus", "", "Read from a given Prometheus")
		readFileF       = flag.String("read-file", "", "Read from a given file")
		// writePromHouseF = flag.String("write-promhouse", "Write to a given PromHouse")
		writeFileF = flag.String("write-file", "", "Write to a given file")
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
			log.Fatal(err)
		}
		defer f.Close()

		r := newFileClient(f)
		reader = r
		defer func() {
			logrus.Debugf(
				"fileClient reader caps: bRead=%d, bDecoded=%d, bMarshaled=%d, bEncoded=%d",
				cap(r.bRead), cap(r.bDecoded), cap(r.bMarshaled), cap(r.bEncoded),
			)
		}()

	case *readPrometheusF != "":
		end := time.Now().Truncate(time.Minute)
		start := end.Add(-time.Duration(lastF))
		r, err := newPrometheusClient(*readPrometheusF, start, end, time.Duration(stepF))
		if err != nil {
			log.Fatal(err)
		}
		reader = r
		defer func() {
			logrus.Debugf(
				"prometheusClient caps: bRead=%d, bDecoded=%d, bMarshaled=%d, bEncoded=%d",
				cap(r.bRead), cap(r.bDecoded), cap(r.bMarshaled), cap(r.bEncoded),
			)
		}()

	default:
		log.Fatal("No -read-* flag given.")
	}

	var writer tsWriter
	switch {
	case *writeFileF != "":
		f, err := os.Create(*writeFileF)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		w := newFileClient(f)
		writer = w
		defer func() {
			logrus.Debugf(
				"fileClient writer caps: bRead=%d, bDecoded=%d, bMarshaled=%d, bEncoded=%d",
				cap(w.bRead), cap(w.bDecoded), cap(w.bMarshaled), cap(w.bEncoded),
			)
		}()

	default:
		log.Fatal("No -write-* flag given.")
	}

	for {
		ts, err := reader.readTS()
		if err != nil {
			log.Printf("%+v", err)
			break
		}

		if err = writer.writeTS(ts); err != nil {
			log.Printf("%+v", err)
			break
		}
	}
}
