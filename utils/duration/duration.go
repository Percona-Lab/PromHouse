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

package duration

import (
	"github.com/prometheus/common/model"
	"gopkg.in/alecthomas/kingpin.v2"
)

// FromFlag is custom kingpin parser for model.Duration flags.
func FromFlag(s kingpin.Settings) *model.Duration {
	d := new(model.Duration)
	s.SetValue(d)
	return d
}
