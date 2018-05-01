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
	"io"
	"log"
	"net/url"
	"os"
	"strings"
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

func parseArg(arg string) (string, string, error) {
	t := strings.Split(arg, ":")[0]
	switch t {
	case "file":
		f := strings.TrimPrefix(arg, t+":")
		return t, f, nil
	case "remote":
		u := strings.TrimPrefix(arg, t+":")
		if _, err := url.Parse(u); err != nil {
			return "", "", err
		}
		return t, u, nil
	default:
		return "", "", fmt.Errorf("unexpected type")
	}
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		fromArg = kingpin.Arg("from", "Read data from that source\n\tfile:data.bin for local file\n\tremote:http://127.0.0.1:7781/read for PromHouse\n\tremote:http://127.0.0.1:9090/api/v1/read for Prometheus").Required().String()
		toArg   = kingpin.Arg("to", "Write data to that destination\n\tfile:data.bin for local file\n\tremote:http://127.0.0.1:7781/write for PromHouse").Required().String()

		lastF = duration.FromFlag(kingpin.Flag("from.remote.last", "Remote source: read from that time ago").Default("30d"))
		stepF = duration.FromFlag(kingpin.Flag("from.remote.step", "Remote source: interval for a single request").Default("1m"))

		logLevelF = kingpin.Flag("log.level", "Log level").Default("warn").String()
	)
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	level, err := logrus.ParseLevel(*logLevelF)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(level)

	var reader tsReader
	var writer tsWriter

	{
		fromType, fromAddr, err := parseArg(*fromArg)
		if err != nil {
			logrus.Fatalf("Failed to parse 'from' argument %s: %s.", *fromArg, err)
		}
		switch fromType {
		case "file":
			f, err := os.Open(fromAddr)
			if err != nil {
				logrus.Fatal(err)
			}
			defer func() {
				if err = f.Close(); err != nil {
					logrus.Error(err)
				}
				logrus.Infof("%s closed.", f.Name())
			}()

			logrus.Infof("Reading metrics from %s %s.", fromType, fromAddr)
			reader = newFileClient(f)

		case "remote":
			end := time.Now().Truncate(time.Minute)
			start := end.Add(-time.Duration(*lastF))

			logrus.Infof("Reading metrics from %s %s between %s and %s with step %s.", fromType, fromAddr, start, end, *stepF)
			reader = newRemoteClient(fromAddr, start, end, time.Duration(*stepF))

		default:
			panic("not reached")
		}
	}

	{
		toType, toAddr, err := parseArg(*toArg)
		if err != nil {
			logrus.Fatalf("Failed to parse 'to' argument %s: %s.", *toArg, err)
		}
		switch toType {
		case "file":
			f, err := os.Create(toAddr)
			if err != nil {
				logrus.Fatal(err)
			}
			defer func() {
				if err = f.Close(); err != nil {
					logrus.Error(err)
				}
				logrus.Infof("%s closed.", f.Name())
			}()

			logrus.Infof("Writing metrics to %s %s.", toType, toAddr)
			writer = newFileClient(f)

		case "remote":
			logrus.Infof("Writing metrics to %s %s.", toType, toAddr)
			writer = newRemoteClient(toAddr, time.Time{}, time.Time{}, 0)

		default:
			panic("not reached")
		}
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
