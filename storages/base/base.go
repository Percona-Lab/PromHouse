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

// Package base provides common storage code.
package base

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/Percona-Lab/PromHouse/prompb"
)

// Storage represents generic storage.
type Storage interface {
	// Read runs queries in the storage and returns the same amount of matrixes.
	// Event if they are empty, they must be present in the returned slice.
	Read(context.Context, []Query) (*prompb.ReadResponse, error)

	// Write puts data into storage.
	Write(context.Context, *prompb.WriteRequest) error

	prometheus.Collector
}

// Query represents query against stored data.
type Query struct {
	Start    model.Time
	End      model.Time
	Matchers Matchers
}

func (q Query) String() string {
	return fmt.Sprintf("[%d,%d,%s]", q.Start, q.End, q.Matchers)
}

type MatchType int

const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (m MatchType) String() string {
	switch m {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	default:
		panic("unknown match type")
	}
}

type Matcher struct {
	Name  string
	Type  MatchType
	Value string
	re    *regexp.Regexp
}

func (m Matcher) String() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value)
}

type Matchers []Matcher

var emptyLabel = &prompb.Label{}

func (ms Matchers) String() string {
	res := make([]string, len(ms))
	for i, m := range ms {
		res[i] = m.String()
	}
	return "{" + strings.Join(res, ",") + "}"
}

func (ms Matchers) MatchLabels(labels []*prompb.Label) bool {
	// TODO if both matchers and labels are sorted by label name, we can optimize that method

	// We expect that from Prometheus (from https://prometheus.io/docs/querying/basics/):
	// * Label matchers that match empty label values also select all time series that do not have the specific label set at all.
	// * At least one matcher should have non-empty label value.
	// Check it.
	if len(ms) == 0 {
		panic("MatchLabels: empty matchers")
	}
	var hasValue bool
	for _, m := range ms {
		if m.Name == "" {
			panic("MatchLabels: empty label name")
		}
		if m.Value != "" {
			hasValue = true
		}
	}
	if !hasValue {
		panic("MatchLabels: all labels has empty values")
	}

	for _, m := range ms {
		if (m.re == nil) && (m.Type == MatchRegexp || m.Type == MatchNotRegexp) {
			m.re = regexp.MustCompile("^(?:" + m.Value + ")$")
		}

		label := emptyLabel
		for _, l := range labels {
			if m.Name == l.Name {
				label = l
				break
			}
		}

		// return false if not matches, continue to the next matcher otherwise
		switch m.Type {
		case MatchEqual:
			if m.Value != label.Value {
				return false
			}
		case MatchNotEqual:
			if m.Value == label.Value {
				return false
			}
		case MatchRegexp:
			if !m.re.MatchString(label.Value) {
				return false
			}
		case MatchNotRegexp:
			if m.re.MatchString(label.Value) {
				return false
			}
		default:
			panic("unknown match type")
		}
	}

	return true
}

// SortLabels sorts labels by name.
func SortLabels(labels []*prompb.Label) {
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
}

// check interfaces
var (
	_ fmt.Stringer = Query{}
	_ fmt.Stringer = MatchType(0)
	_ fmt.Stringer = Matcher{}
	_ fmt.Stringer = Matchers{}
)
