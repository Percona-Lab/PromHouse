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

// +build gofuzz

package clickhouse

import (
	"encoding/json"
	"log"
	"reflect"

	"github.com/prometheus/common/model"

	"github.com/Percona-Lab/PromHouse/storages/base"
)

func FuzzJSON(data []byte) int {
	// compare unmarshalers
	labels1, err := unmarshalLabels(data) // does not preserves an order
	m := make(map[string]string)
	e := json.Unmarshal(data, &m)
	if err != nil && e != nil {
		return 0
	}
	if err != nil {
		panic(err)
	}
	if e != nil {
		panic(err)
	}

	// check if label names are valid
	for _, l := range labels1 {
		if !model.LabelNameRE.MatchString(l.Name) {
			return 0
		}
	}

	// marshal and unmarshal
	data = marshalLabels(labels1, nil)
	labels2, err := unmarshalLabels(data) // does not preserves an order
	if err != nil {
		panic(err)
	}

	base.SortLabels(labels1)
	base.SortLabels(labels2)
	if !reflect.DeepEqual(labels1, labels2) {
		log.Printf("labels1 = %+v", labels1)
		log.Printf("labels2 = %+v", labels2)
		panic("not equal")
	}

	return 1
}
