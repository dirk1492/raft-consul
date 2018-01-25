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
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

var utilTestData = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}

func Test_getIndex(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		key     string
		want    uint64
		wantErr bool
	}{
		{
			name:    "Empty key",
			key:     "",
			wantErr: true,
		},
		{
			name:    "Simple Value",
			key:     "1234",
			want:    1234,
			wantErr: false,
		},
		{
			name:    "Parse error 1",
			key:     "1234x",
			wantErr: true,
		},
		{
			name:    "Parse error 2",
			key:     "x1234",
			wantErr: true,
		},
		{
			name:    "Simple key",
			key:     "/test/1234",
			want:    1234,
			wantErr: false,
		},
		{
			name:    "Key without a index",
			key:     "/test/",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getIndex(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("str2Uint64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("str2Uint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getKey(t *testing.T) {
	max := ^uint64(0)

	tests := []struct {
		name   string
		index  uint64
		prefix string
		want   string
	}{
		{
			name:   "",
			index:  1,
			prefix: "/raft/logs/",
			want:   "/raft/logs/00000000000000000001",
		},
		{
			name:   "",
			index:  max,
			prefix: "/raft/logs/",
			want:   "/raft/logs/18446744073709551615",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getKey(tt.index, tt.prefix)

			if got != tt.want {
				t.Errorf("getKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_split(t *testing.T) {

	firstIndex := uint64(726545)

	tests := []struct {
		name  string
		index uint64
		count uint64
		split int
		term  uint64
		ltype raft.LogType
		data  []byte
	}{
		{
			name:  "Split 64 logs",
			index: firstIndex + 1,
			count: 64,
			split: 0,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  utilTestData,
		},
		{
			name:  "Split 64 logs",
			index: firstIndex + 1,
			count: 64,
			split: 64,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  utilTestData,
		},
		{
			name:  "Split 65 logs",
			index: firstIndex + 1,
			count: 65,
			split: 64,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  utilTestData,
		},
		{
			name:  "Split 10 logs in chunks of size 1",
			index: firstIndex + 1,
			count: 10,
			split: 1,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  utilTestData,
		},
		{
			name:  "Split 0 logs",
			index: firstIndex + 1,
			count: 0,
			split: 64,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  utilTestData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			logs := []*raft.Log{}
			for i := uint64(0); i < tt.count; i++ {
				log := raft.Log{
					Index: tt.index + i,
					Term:  tt.term,
					Type:  tt.ltype,
					Data:  tt.data,
				}

				logs = append(logs, &log)
			}

			got := split(logs, tt.split)
			lgot := len(got)

			if tt.split == 0 {
				if got != nil {
					t.Errorf("split(): with chunk size <1 should return nil")
				}
			} else {
				l := len(logs)
				rest := l % tt.split
				chunks := int(math.Floor(float64(l-1)/float64(tt.split))) + 1

				if lgot != chunks {
					t.Errorf("split(): wrong chunk count %v, want %v", lgot, chunks)
				}

				if rest == 0 {
					for i := 0; i < lgot; i++ {
						if len(got[i]) != tt.split {
							t.Errorf("split(): wrong chunk size %v, want %v", len(got[i]), tt.split)
						}
					}
				} else {
					for i := 0; i < lgot-1; i++ {
						if len(got[i]) != tt.split {
							t.Errorf("split(): wrong chunk size %v, want %v", len(got[i]), tt.split)
						}
					}

					lastChunkSize := len(got[lgot-1])
					if lastChunkSize != rest {
						t.Errorf("split(): wrong chunk size %v, want %v", lastChunkSize, rest)
					}
				}
			}

		})
	}
}

func TestUint64Array_Len(t *testing.T) {
	tests := []struct {
		name string
		a    Uint64Array
		want int
	}{
		{
			"Empty array",
			Uint64Array{},
			0,
		},
		{
			"Normal array",
			Uint64Array{1, 2, 3, 4},
			4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Len(); got != tt.want {
				t.Errorf("Unit64Array.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUint64Array_Swap(t *testing.T) {
	tests := []struct {
		name    string
		a       Uint64Array
		pos1    int
		pos2    int
		want    Uint64Array
		wantErr bool
	}{
		{
			name: "Swap 2 items",
			a:    Uint64Array{1, 2, 3, 4, 5},
			pos1: 1,
			pos2: 3,
			want: Uint64Array{1, 4, 3, 2, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.a.Swap(tt.pos1, tt.pos2)

			if !reflect.DeepEqual(tt.a, tt.want) {
				t.Errorf("expected %v, got %v", tt.a, tt.want)
			}
		})
	}
}

func TestUint64Array_Less(t *testing.T) {
	tests := []struct {
		name string
		a    Uint64Array
		i    int
		j    int
		want bool
	}{
		{
			a:    Uint64Array{1, 2, 3, 4, 5},
			i:    1,
			j:    2,
			want: true,
		},
		{
			a:    Uint64Array{1, 2, 3, 4, 5},
			i:    1,
			j:    1,
			want: false,
		},
		{
			a:    Uint64Array{1, 2, 3, 4, 5},
			i:    4,
			j:    2,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Less(tt.i, tt.j); got != tt.want {
				t.Errorf("Uint64Array.Less() = %v, want %v", got, tt.want)
			}
		})
	}
}
