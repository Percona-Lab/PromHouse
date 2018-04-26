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

// We need Go 1.9+ for proper uint64 handling: https://github.com/kshvakov/clickhouse/issues/69
// +build go1.9

package main

import (
	"bytes"
	"context"
	_ "expvar"
	"html/template"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/Percona-Lab/PromHouse/handlers"
	"github.com/Percona-Lab/PromHouse/storages/clickhouse"
)

const (
	shutdownTimeout = 3 * time.Second
)

// runPromServer runs Prometheus API server until context is canceled, then gracefully stops it.
func runPromServer(ctx context.Context, addr string, drop bool) {
	l := logrus.WithField("component", "api")

	storage, err := clickhouse.New("tcp://127.0.0.1:9000", "prometheus", drop)
	if err != nil {
		l.Panic(err)
	}

	prometheus.MustRegister(storage)

	promAPI := handlers.NewPromAPI(storage, l)
	prometheus.MustRegister(promAPI)

	mux := http.NewServeMux()
	mux.HandleFunc("/read", promAPI.Read)
	mux.HandleFunc("/write", promAPI.Write)

	l.Printf("Starting server on http://%s/", addr)
	server := &http.Server{
		Addr:     addr,
		ErrorLog: log.New(os.Stderr, "runPromServer: ", 0),
		Handler:  mux,
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Print("Server stopped.")
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
		"/debug/metrics", "/debug/vars", "/debug/requests", "/debug/events", "/debug/pprof",
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
	l.Printf("Starting server on http://%s/debug\nRegistered handlers:\n\t%s", addr, strings.Join(handlers, "\n\t"))

	server := &http.Server{
		Addr:     addr,
		ErrorLog: log.New(os.Stderr, "runDebugServer: ", 0),
	}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			l.Panic(err)
		}
		l.Print("Server stopped.")
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
		promAddrF  = kingpin.Flag("listen-prom-addr", "Prometheus remote API server listen address").Default("127.0.0.1:7781").String()
		debugAddrF = kingpin.Flag("listen-debug-addr", "Debug server listen address").Default("127.0.0.1:7782").String()
		debugF     = kingpin.Flag("debug", "Enable debug logging").Bool()
		dropF      = kingpin.Flag("drop", "Drop existing ClickHouse schema").Bool()
	)
	kingpin.Parse()

	logrus.SetLevel(logrus.InfoLevel)
	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}

	l := logrus.WithField("component", "main")
	ctx, cancel := context.WithCancel(context.Background())
	defer l.Print("Done.")

	// handle termination signals: first one gracefully, force exit on the second one
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-signals
		l.Warnf("Got %v (%d) signal, shutting down...", s, s)
		cancel()

		s = <-signals
		l.Panicf("Got %v (%d) signal, exiting!", s, s)
	}()

	// start servers, wait for them to exit
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runPromServer(ctx, *promAddrF, *dropF)
	}()
	go func() {
		defer wg.Done()
		runDebugServer(ctx, *debugAddrF)
	}()
	wg.Wait()
}
