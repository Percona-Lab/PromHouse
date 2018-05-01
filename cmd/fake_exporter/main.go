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
	"log"
	"net/http"

	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		upstreamF         = kingpin.Flag("upstream", "Upstream exporter metrics endpoint").Default("http://127.0.0.1:9100/metrics").String()
		instanceTemplateF = kingpin.Flag("instance-template", "Instance label value template").Default("multi%d").String()
		instancesF        = kingpin.Flag("instances", "Number of instances to generate").Default("100").Int()
		logLevelF         = kingpin.Flag("log.level", "Log level").Default("warn").String()
		listenF           = kingpin.Flag("web.listen-address", "Address on which to expose metrics").Default("127.0.0.1:9099").String()
	)
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	level, err := logrus.ParseLevel(*logLevelF)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(level)

	client := newClient(*upstreamF)
	faker := newFaker(*instanceTemplateF, *instancesF)
	http.HandleFunc("/metrics", func(rw http.ResponseWriter, req *http.Request) {
		r, err := client.get()
		if err != nil {
			logrus.Error(err)
			http.Error(rw, err.Error(), 500)
			return
		}

		if err = faker.generate(rw, r); err != nil {
			logrus.Error(err)
		}
	})

	logrus.Infof("Listening on http://%s/metrics ...", *listenF)
	logrus.Fatal(http.ListenAndServe(*listenF, nil))
}
