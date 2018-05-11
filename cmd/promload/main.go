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
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Percona-Lab/PromHouse/prompb"
	"github.com/Percona-Lab/PromHouse/utils/duration"
)

type readProgress struct {
	current, max uint
}

type tsReader interface {
	readTS() ([]*prompb.TimeSeries, *readProgress, error)
	close() error
}

type tsWriter interface {
	writeTS(ts []*prompb.TimeSeries) error
	close() error
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
		// remote:http://127.0.0.1:9090/api/v1/read for Prometheus
		sourceArg      = kingpin.Arg("source", "Read data from that source\n\tfile:data.bin for local file\n\tremote:http://127.0.0.1:7781/read for remote storage").Required().String()
		destinationArg = kingpin.Arg("destination", "Write data to that destination\n\tfile:data.bin for local file\n\tremote:http://127.0.0.1:7781/write for remote storage").Required().String()

		lastF = duration.FromFlag(kingpin.Flag("source.remote.last", "Remote source: read from that time ago").Default("30d"))
		stepF = duration.FromFlag(kingpin.Flag("source.remote.step", "Remote source: interval for a single request").Default("1m"))

		logLevelF = kingpin.Flag("log.level", "Log level").Default("info").String()
	)
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	level, err := logrus.ParseLevel(*logLevelF)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(level)

	ctx, cancel := context.WithCancel(context.Background())
	defer logrus.Print("Done.")

	// handle termination signals: first one gracefully, force exit on the second one
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-signals
		logrus.Warnf("Got %s, shutting down...", unix.SignalName(s.(syscall.Signal)))
		cancel()

		s = <-signals
		logrus.Panicf("Got %s, exiting!", s, unix.SignalName(s.(syscall.Signal)))
	}()

	var reader tsReader
	var writer tsWriter

	{
		sourceType, sourceAddr, err := parseArg(*sourceArg)
		if err != nil {
			logrus.Fatalf("Failed to parse 'source' argument %s: %s.", *sourceArg, err)
		}
		switch sourceType {
		case "file":
			f, err := os.Open(sourceAddr)
			if err != nil {
				logrus.Fatal(err)
			}
			logrus.Infof("Reading metrics from %s %s.", sourceType, sourceAddr)
			reader = newFileClient(f)

		case "remote":
			end := time.Now().Truncate(time.Minute)
			start := end.Add(-time.Duration(*lastF))
			logrus.Infof("Reading metrics from %s %s between %s and %s with step %s.", sourceType, sourceAddr, start, end, *stepF)
			reader = newRemoteClient(sourceAddr, start, end, time.Duration(*stepF))

		default:
			panic("not reached")
		}
	}

	{
		destinationType, destinationAddr, err := parseArg(*destinationArg)
		if err != nil {
			logrus.Fatalf("Failed to parse 'destination' argument %s: %s.", *destinationArg, err)
		}
		switch destinationType {
		case "file":
			f, err := os.Create(destinationAddr)
			if err != nil {
				logrus.Fatal(err)
			}
			logrus.Infof("Writing metrics to %s %s.", destinationType, destinationAddr)
			writer = newFileClient(f)

		case "remote":
			logrus.Infof("Writing metrics to %s %s.", destinationType, destinationAddr)
			writer = newRemoteClient(destinationAddr, time.Time{}, time.Time{}, 0)

		default:
			panic("not reached")
		}
	}

	ch := make(chan []*prompb.TimeSeries, 100)
	var lastReport time.Time
	go func() {
		for {
			ts, rp, err := reader.readTS()
			if err == nil {
				err = ctx.Err()
			}
			if err != nil {
				if err != io.EOF {
					logrus.Errorf("Read error: %+v", err)
				}
				if err = reader.close(); err != nil {
					logrus.Errorf("Reader close error: %+v", err)
				}
				close(ch)
				return
			}

			if rp != nil && rp.max > 0 {
				if time.Since(lastReport) > 10*time.Second {
					lastReport = time.Now()
					logrus.Infof("Read %.2f%% (%d / %d), write buffer: %d / %d.",
						float64(rp.current*100)/float64(rp.max), rp.current, rp.max, len(ch), cap(ch))
				}
			}

			ch <- ts
		}
	}()

	for ts := range ch {
		if err := writer.writeTS(ts); err != nil {
			logrus.Errorf("Write error: %+v", err)
		}
	}
	if err = writer.close(); err != nil {
		logrus.Errorf("Writer close error: %+v", err)
	}
}
