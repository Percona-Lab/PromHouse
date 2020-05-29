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
