// Copyright 2018 Dirk Lembke

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftconsul

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/hashicorp/raft"
)

func decode(buf []byte, log *raft.Log) error {
	return json.Unmarshal(buf, &log)
}

func encode(in interface{}) ([]byte, error) {
	rc, err := json.Marshal(in)

	if err != nil {
		return nil, err
	}

	return []byte(rc), nil
}

func getIndex(key string) (uint64, error) {
	pos := strings.LastIndexByte(key, '/')

	if pos != -1 {
		key = key[pos+1:]
	}

	return strconv.ParseUint(key, 10, 64)
}

func getKey(index uint64, prefix string) string {
	return fmt.Sprintf("%v%020d", prefix, index)
	//prefix + strconv.FormatUint(index, 10)
}

func split(logs []*raft.Log, size int) [][]*raft.Log {

	if size < 1 {
		return nil
	}

	l := len(logs)
	rest := l % size
	chunks := int(math.Floor(float64(l-1)/float64(size))) + 1
	rc := make([][]*raft.Log, chunks, chunks)

	for i := 0; i < chunks; i++ {
		var chunk []*raft.Log

		if i != chunks-1 || rest == 0 {
			chunk = logs[i*size : (i+1)*size]
		} else {
			chunk = logs[i*size : i*size+rest]
		}

		rc[i] = chunk
	}

	return rc
}

// Uint64Array is a sortable uint64 array
type Uint64Array []uint64

func (a Uint64Array) Len() int           { return len(a) }
func (a Uint64Array) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Uint64Array) Less(i, j int) bool { return a[i] < a[j] }
