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

// +build gofuzzgen

package gofuzz

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
)

var root string

func init() {
	_, file, _, _ := runtime.Caller(0)
	root = filepath.Join(filepath.Dir(file), "..", "..", "go-fuzz")
}

// AddToCorpus adds data to named go-fuzz corpus when "gofuzzgen" build tag is used.
func AddToCorpus(name string, data []byte) {
	path := filepath.Join(root, name, "corpus")
	_ = os.MkdirAll(path, 0777)

	path = filepath.Join(path, fmt.Sprintf("test-%x", sha1.Sum(data)))
	if err := ioutil.WriteFile(path, data, 0666); err != nil {
		panic(err)
	}
}
