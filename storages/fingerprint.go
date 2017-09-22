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

// Code in this file is adopted from the following files:
// * vendor/github.com/prometheus/common/model/fnv.go
// * vendor/github.com/prometheus/common/model/signature.go
// Original license below:

// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storages

import (
	"github.com/Percona-Lab/PromHouse/prompb"
)

const (
	offset64      uint64 = 14695981039346656037
	prime64       uint64 = 1099511628211
	separatorByte byte   = 255
)

// hashAdd adds a string to a fnv64a hash value, returning the updated hash.
func hashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

// fingerprint calculates a fingerprint of SORTED BY NAME labels.
// It is adopted from labelSetToFingerprint, but avoids type conversions and memory allocations.
func fingerprint(labels []*prompb.Label) uint64 {
	if len(labels) == 0 {
		return offset64
	}

	sum := offset64
	for _, l := range labels {
		sum = hashAdd(sum, l.Name)
		sum = hashAddByte(sum, separatorByte)
		sum = hashAdd(sum, l.Value)
		sum = hashAddByte(sum, separatorByte)
	}
	return sum
}
