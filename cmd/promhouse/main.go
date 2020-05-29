// Copyright 2017, 2018 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	_ "expvar" // for /debug/expvar
	"fmt"
	"html/template"
	"log"
	"net/http"
	_ "net/http/pprof" // for /debug/pprof
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Percona-Lab/PromHouse/handlers"
	"github.com/Percona-Lab/PromHouse/storages/base"
	"github.com/Percona-Lab/PromHouse/storages/blackhole"
	"github.com/Percona-Lab/PromHouse/storages/clickhouse"
	"github.com/Percona-Lab/PromHouse/storages/memory"
)

const (
	shutdownTimeout = 3 * time.Second
)

// runPromServer runs Prometheus API server until context is canceled, then gracefully stops it.
func runPromServer(ctx context.Context, addr string, storageType string, params *clickhouse.Params) {
	l := logrus.WithField("component", "api")

	var storage base.Storage
	var err error
	switch storageType {
	case "blackhole":
		storage = blackhole.New()
	case "clickhouse":
		storage, err = clickhouse.New(params)
	case "memory":
		storage = memory.New()
	default:
		err = fmt.Errorf("unhandled storage type %q", storageType)
	}
	if err != nil {
		l.Panic(err)
	}

	prometheus.MustRegister(storage)

	promAPI := handlers.NewPromAPI(storage, l)
	prometheus.MustRegister(promAPI)

	mux := http.NewServeMux()
	//mux.HandleFunc("/read", promAPI.Read())
	mux.HandleFunc("/write", promAPI.Write())

	l.Infof("Starting server on http://%s/", addr)
	server := &http.Server{
		Addr:     addr,
		ErrorLog: log.New(os.Stderr, "runPromServer: ", 0),
		Handler:  mux,
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Info("Server stopped.")
	}()

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := server.Shutdown(ctx); err != nil {
		l.Errorf("Failed to shutdown gracefully: %s", err)
	}
	cancel()
}

// runDebugServer runs debug server until context is canceled, then gracefully stops it.
func runDebugServer(ctx context.Context, addr string) {
	l := logrus.WithField("component", "debug")

	http.Handle("/debug/metrics", promhttp.Handler())

	handlers := []string{
		"/debug/metrics", "/debug/vars", "/debug/pprof",
	}
	for i, h := range handlers {
		handlers[i] = "http://" + addr + h
	}

	var buf bytes.Buffer
	err := template.Must(template.New("debug").Parse(`
	<html>
	<body>
	<ul>
	{{ range . }}
		<li><a href="{{ . }}">{{ . }}</a></li>
	{{ end }}
	</ul>
	</body>
	</html>
	`)).Execute(&buf, handlers)
	if err != nil {
		l.Panic(err)
	}
	http.HandleFunc("/debug", func(rw http.ResponseWriter, req *http.Request) {
		rw.Write(buf.Bytes())
	})
	l.Infof("Starting server on http://%s/debug\nRegistered handlers:\n\t%s", addr, strings.Join(handlers, "\n\t"))

	server := &http.Server{
		Addr:     addr,
		ErrorLog: log.New(os.Stderr, "runDebugServer: ", 0),
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Info("Server stopped.")
	}()

	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := server.Shutdown(ctx); err != nil {
		l.Errorf("Failed to shutdown gracefully: %s", err)
	}
	cancel()
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		promAddrF     = kingpin.Flag("listen-prom-addr", "Prometheus remote API server listen address").Default("127.0.0.1:7781").String()
		debugAddrF    = kingpin.Flag("listen-debug-addr", "Debug server listen address").Default("127.0.0.1:7782").String()
		dropF         = kingpin.Flag("db.drop-schema", "Drop existing database schema").Bool()
		maxOpenConnsF = kingpin.Flag("db.max-open-conns", "Maximum number of open connections to the database").Default("75").Int()
		storageTypeF  = kingpin.Flag("storage-type", "Storage type").Default("clickhouse").String()
		logLevelF     = kingpin.Flag("log.level", "Log level").Default("warn").String()
	)
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	level, err := logrus.ParseLevel(*logLevelF)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(level)

	l := logrus.WithField("component", "main")
	ctx, cancel := context.WithCancel(context.Background())
	defer l.Info("Done.")

	// handle termination signals: first one gracefully, force exit on the second one
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-signals
		l.Warnf("Got %s, shutting down...", unix.SignalName(s.(syscall.Signal)))
		cancel()

		s = <-signals
		l.Panicf("Got %s, exiting!", unix.SignalName(s.(syscall.Signal)))
	}()

	// start servers, wait for them to exit
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		params := &clickhouse.Params{
			DSN:                  "tcp://127.0.0.1:9000/?database=prometheus",
			DropDatabase:         *dropF,
			MaxOpenConns:         *maxOpenConnsF,
			MaxTimeSeriesInQuery: 50,
		}
		runPromServer(ctx, *promAddrF, *storageTypeF, params)
	}()
	go func() {
		defer wg.Done()
		runDebugServer(ctx, *debugAddrF)
	}()
	wg.Wait()
}
