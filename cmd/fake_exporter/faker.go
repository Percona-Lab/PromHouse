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
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// faker generates new fake metrics based on upstream metrics.
type faker struct {
	instanceTemplate string
	instances        int
	sort             bool // set to true for stable results (for example, in tests)

	m   sync.Mutex
	rnd *rand.Rand
}

func newFaker(instanceTemplate string, instances int) *faker {
	return &faker{
		instanceTemplate: instanceTemplate,
		instances:        instances,
		rnd:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// fakeValue slightly changes upstream's value, handling special cases.
func (f *faker) fakeValue(vp *float64) {
	v := *vp

	// do not change special values
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return
	}

	// do not change exact 0 and 1 (for example, metrics like mysql_up, or number of errors which is almost always 0)
	if v == 0.0 || v == 1.0 {
		return
	}

	// add ±10%
	f.m.Lock()
	v += v * (f.rnd.Float64()*0.2 - 0.1)
	f.m.Unlock()

	// do not change integer to float
	if *vp == math.Trunc(*vp) {
		v = math.Trunc(v)
	}

	*vp = v
}

// generate reads upstream metrics from src, generates new metrics and writes them to dst.
func (f *faker) generate(dst io.Writer, src io.Reader) error {
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

			if f.sort {
				sort.Slice(m.Label, func(i, j int) bool { return *m.Label[i].Name < *m.Label[j].Name })
			}
		}

		upstream = append(upstream, mf)
	}
	if f.sort {
		sort.Slice(upstream, func(i, j int) bool { return *upstream[i].Name < *upstream[j].Name })
	}

	// encode new metrics, grouping by metric family and instance
	e := expfmt.NewEncoder(dst, expfmt.FmtText)
	for _, mf := range upstream {
		nmf := &dto.MetricFamily{
			Name:   mf.Name,
			Help:   mf.Help,
			Type:   mf.Type,
			Metric: make([]*dto.Metric, len(mf.Metric)*f.instances),
		}
		for instance := 0; instance < f.instances; instance++ {
			for i, m := range mf.Metric {
				nm := proto.Clone(m).(*dto.Metric)
				nm.Label[len(nm.Label)-1].Value = proto.String(fmt.Sprintf(f.instanceTemplate, instance+1))

				switch *mf.Type {
				case dto.MetricType_COUNTER:
					f.fakeValue(nm.Counter.Value)
				case dto.MetricType_GAUGE:
					f.fakeValue(nm.Gauge.Value)
				case dto.MetricType_UNTYPED:
					f.fakeValue(nm.Untyped.Value)
				default:
					// skip histograms and summaries for now
				}

				nmf.Metric[instance*len(mf.Metric)+i] = nm
			}
		}
		if err := e.Encode(nmf); err != nil {
			return err
		}
	}

	return nil
}
