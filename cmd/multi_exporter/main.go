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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type client struct {
	exporter string
	http     *http.Client
}

func newClient(exporter string) *client {
	return &client{
		exporter: exporter,
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 100,
			},
		},
	}
}

func (c *client) get() (io.Reader, error) {
	req, err := http.NewRequest("GET", c.exporter, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, b)
	}
	return bytes.NewReader(b), nil
}

func multi(dst io.Writer, src io.Reader, instanceTemplate string, instances int) error {
	// decode upstream metrics, add instance label placeholder
	d := expfmt.NewDecoder(src, expfmt.FmtText)
	upstream := make([]*dto.MetricFamily, 0, 1000)
	for {
		mf := new(dto.MetricFamily)
		if err := d.Decode(mf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		for _, m := range mf.Metric {
			m.Label = append(m.Label, &dto.LabelPair{
				Name: proto.String("instance"),
			})
		}

		upstream = append(upstream, mf)
	}

	// encode new metrics, grouping by metric family and instance
	e := expfmt.NewEncoder(dst, expfmt.FmtText)
	for _, mf := range upstream {
		nmf := &dto.MetricFamily{
			Name:   mf.Name,
			Help:   mf.Help,
			Type:   mf.Type,
			Metric: make([]*dto.Metric, len(mf.Metric)*instances),
		}
		for instance := 0; instance < instances; instance++ {
			for i, m := range mf.Metric {
				nm := proto.Clone(m).(*dto.Metric)
				nm.Label[len(nm.Label)-1].Value = proto.String(fmt.Sprintf(instanceTemplate, instance+1))

				// TODO tweak nm value

				nmf.Metric[instance*len(mf.Metric)+i] = nm
			}
		}
		if err := e.Encode(nmf); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("stdlog: ")

	var (
		upstreamF         = kingpin.Flag("upstream", "Upstream exporter metrics endpoint").Default("http://127.0.0.1:9100/metrics").String()
		instanceTemplateF = kingpin.Flag("instance-template", "Instance label value template").Default("multi%d").String()
		instancesF        = kingpin.Flag("instances", "Number of instances to generate").Default("100").Int()
		debugF            = kingpin.Flag("debug", "Enable debug output").Bool()
		listenF           = kingpin.Flag("web.listen-address", "Address on which to expose metrics").Default("127.0.0.1:9099").String()
	)
	kingpin.Parse()

	logrus.SetLevel(logrus.InfoLevel)
	if *debugF {
		logrus.SetLevel(logrus.DebugLevel)
	}

	client := newClient(*upstreamF)
	http.HandleFunc("/metrics", func(rw http.ResponseWriter, req *http.Request) {
		r, err := client.get()
		if err != nil {
			logrus.Error(err)
			http.Error(rw, err.Error(), 500)
			return
		}

		if err = multi(rw, r, *instanceTemplateF, *instancesF); err != nil {
			logrus.Error(err)
		}
	})

	logrus.Infof("Listening on http://%s/metrics ...", *listenF)
	logrus.Fatal(http.ListenAndServe(*listenF, nil))
}
