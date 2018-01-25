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
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestInterfaceImpl(t *testing.T) {
	var logStore raft.LogStore
	var stableStore raft.StableStore

	tmp := new(ConsulStore)

	logStore = tmp
	stableStore = tmp

	_ = logStore
	_ = stableStore
}

func TestNewConsulStore(t *testing.T) {
	type args struct {
		config Config
	}
	tests := []struct {
		name    string
		host    string
		timeout time.Duration
		want    *ConsulStore
		wantErr bool
	}{
		{
			name:    "Create new store",
			host:    "localhost",
			timeout: 5 * time.Minute,
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Timeout: tt.timeout,
			}

			_, err := NewConsulStore(config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsulStore() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("NewConsulStore() = %v, want %v", got, tt.want)
				}
			*/
		})
	}
}

func TestConsulStore_Clear(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		arg     string
		wantErr bool
	}{
		{
			name: "clear default tree",
		},
		{
			name:   "clear custom tree",
			prefix: "custom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			s, err := NewConsulStore(config)
			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", tt.host)
			}

			key := "dummy"
			value := []byte("1234567890")

			err = s.set(key, value, tt.arg)
			if err != nil || s == nil {
				t.Errorf("ConsulStore.Set(%v,%v, %v) failed", key, value, tt.arg)
			}

			if err := s.Clear(tt.arg); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.Clear(%v) error = %v, wantErr %v", tt.arg, err, tt.wantErr)
			}
		})
	}
}

func TestConsulStore_Set(t *testing.T) {
	type args struct {
		key string
		v   []byte
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		key     string
		value   []byte
		wantErr bool
	}{
		{
			name:    "Set test1 = 99",
			host:    "localhost:8500",
			key:     "test1",
			value:   []byte("99"),
			wantErr: false,
		},
		{
			name:    "Set path",
			host:    "localhost:8500",
			key:     "test/tmp/xyz/t1",
			value:   []byte{0x01, 0x02},
			wantErr: false,
		},
		{
			name:    "Custom prefix 1",
			host:    "localhost:8500",
			prefix:  "custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 2",
			host:    "localhost:8500",
			prefix:  "/custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 3",
			host:    "localhost:8500",
			prefix:  "custom/",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := clear(tt.host)
			if err != nil {
				t.Errorf("clear(%v) failed", tt.host)
			}

			s, err := NewConsulStore(config)
			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", tt.host)
			}

			if tt.key == "" {
				tt.key = tt.name
			}

			if tt.value == nil {
				tt.value = []byte(tt.name)
			}

			if err := s.Set([]byte(tt.key), tt.value); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.Set(%v,%v) error = %v, wantErr %v", tt.key, tt.value, err, tt.wantErr)
			}

			if tt.prefix == "" {
				tt.prefix = s.prefix
			}

			err = check(tt.host, tt.prefix, tt.key, tt.value)

			if err != nil {
				t.Errorf("check failed: %v", err)
			}
		})
	}
}

func TestConsulStore_set(t *testing.T) {
	type args struct {
		key string
		v   []byte
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		cprefix string
		key     string
		value   []byte
		wantErr bool
	}{
		{
			name:    "Set test1 = 99",
			host:    "localhost:8500",
			key:     "test1",
			value:   []byte("99"),
			wantErr: false,
		},
		{
			name:    "Set path",
			host:    "localhost:8500",
			key:     "test/tmp/xyz/t1",
			value:   []byte{0x01, 0x02},
			wantErr: false,
		},
		{
			name:    "Custom prefix 1",
			host:    "localhost:8500",
			prefix:  "custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 2",
			host:    "localhost:8500",
			prefix:  "/custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 3",
			host:    "localhost:8500",
			prefix:  "custom/",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 4",
			host:    "localhost:8500",
			cprefix: "custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 5",
			host:    "localhost:8500",
			cprefix: "/custom",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
		{
			name:    "Custom prefix 6",
			host:    "localhost:8500",
			cprefix: "custom/",
			value:   []byte("alea iacta est"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := clear(tt.host)
			if err != nil {
				t.Errorf("clear(%v) failed", tt.host)
			}

			s, err := NewConsulStore(config)
			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", tt.host)
			}

			if tt.key == "" {
				tt.key = tt.name
			}

			if tt.value == nil {
				tt.value = []byte(tt.name)
			}

			if err := s.set(tt.key, tt.value, tt.cprefix); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.Set(%v,%v,%v) error = %v, wantErr %v", tt.key, tt.value, tt.cprefix, err, tt.wantErr)
			}

			if tt.cprefix == "" {
				tt.cprefix = s.prefix
			}

			prefix := tt.cprefix

			err = check(tt.host, prefix, tt.key, tt.value)

			if err != nil {
				t.Errorf("check failed: %v", err)
			}
		})
	}
}

func TestConsulStore_Get(t *testing.T) {
	type args struct {
		key string
		v   []byte
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		key     string
		want    []byte
		wantErr bool
	}{
		{
			name:    "Get key from default prefix",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from subpath with default prefix",
			key:     "test/dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix",
			prefix:  "custom",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from subpath with custom prefix",
			prefix:  "custom",
			key:     "test/dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix with leading slash",
			prefix:  "/custom",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix with trailing slash",
			prefix:  "custom/",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get unknown entry",
			host:    "localhost:8500",
			key:     "unknown",
			want:    []byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.host == "" {
				tt.host = "localhost:8500"
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			s, err := NewConsulStore(config)
			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if tt.key == "" {
				tt.key = tt.name
			}

			value, err := s.Get([]byte(tt.key))

			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.Get(%v) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			}

			if !reflect.DeepEqual(value, tt.want) {
				t.Errorf("Get(%v) = %v, want %v", tt.key, value, tt.want)
			}

		})
	}
}

func TestConsulStore_get(t *testing.T) {
	type args struct {
		key string
		v   []byte
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		cprefix string
		key     string
		want    []byte
		wantErr bool
	}{
		{
			name:    "Get key from default prefix",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from subpath with default prefix",
			key:     "test/dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix",
			prefix:  "custom",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from subpath with custom prefix",
			prefix:  "custom",
			key:     "test/dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix with leading slash",
			prefix:  "/custom",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom prefix with trailing slash",
			prefix:  "custom/",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from subpath func with custom prefix",
			cprefix: "custom",
			key:     "test/dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom func prefix with leading slash",
			cprefix: "/custom",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get key from custom func prefix with trailing slash",
			cprefix: "custom/",
			key:     "dataset1",
			want:    testData,
			wantErr: false,
		},
		{
			name:    "Get unknown entry",
			host:    "localhost:8500",
			key:     "unknown",
			want:    []byte{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.host == "" {
				tt.host = "localhost:8500"
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			s, err := NewConsulStore(config)
			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if tt.key == "" {
				tt.key = tt.name
			}

			value, err := s.get(tt.key, tt.cprefix)

			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.Get(%v,%v) error = %v, wantErr %v", tt.key, tt.cprefix, err, tt.wantErr)
			}

			if !reflect.DeepEqual(value, tt.want) {
				t.Errorf("Get() = %v, want %v", value, tt.want)
			}

		})
	}
}

func TestConsulStore_SetUint64(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		key     string
		val     uint64
		want    []byte
		wantErr bool
	}{
		{
			name: "Simple test",
			key:  "value1",
			val:  4701276917461784236,
			want: []byte("4701276917461784236"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.host == "" {
				tt.host = "localhost:8500"
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("clear(%v) failed", tt.host)
			}

			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if err := s.SetUint64([]byte(tt.key), tt.val); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.SetUint64(%v,%v) error = %v, wantErr %v", tt.key, tt.val, err, tt.wantErr)
			}

			err = check(tt.host, s.prefix, tt.key, tt.want)
			if err != nil {
				t.Errorf("check failed: %v", err)
			}

		})
	}
}

func TestConsulStore_GetUint64(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		key     string
		want    uint64
		wantErr bool
	}{
		{
			name: "Get uint64 value from root",
			key:  "uint64",
			want: uint64TestValue,
		},
		{
			name: "Get uint64 value from root with non ascii key",
			key:  "byte_" + byteKey,
			want: uint64TestValue,
		},
		{
			name: "Get uint64 value from subpath",
			key:  "uint64",
			want: uint64TestValue,
		},
		{
			name: "Unknown entry",
			key:  "abcdef",
			want: 0,
		},
		{
			name:    "Invalid entry",
			key:     "abc",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.host == "" {
				tt.host = "localhost:8500"
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			val, err := s.GetUint64([]byte(tt.key))

			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.SetUint64() error = %v, wantErr %v", err, tt.wantErr)
			}

			if val != tt.want {
				t.Errorf("ConsulStore.SetUint64(%v) = %v, want %v", tt.key, val, tt.want)
			}
		})
	}
}

func TestConsulStore_StoreLog(t *testing.T) {

	type args struct {
		log *raft.Log
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		index   uint64
		term    uint64
		ltype   raft.LogType
		data    []byte
		wantErr bool
	}{
		{
			index: 3464426,
			term:  7369146914,
			ltype: raft.LogNoop,
			data:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			log := raft.Log{
				Index: tt.index,
				Term:  tt.term,
				Type:  tt.ltype,
				Data:  tt.data,
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if err := s.StoreLog(&log); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.StoreLog() error = %v, wantErr %v", err, tt.wantErr)
			}

			tmp, _ := json.Marshal(log)
			err = check(tt.host, s.prefix, fmt.Sprintf("%v%020d", "logs/", tt.index), tmp)
			if err != nil {
				t.Errorf("check failed: %v", err)
			}
		})
	}
}

func TestConsulStore_StoreLogs(t *testing.T) {

	type args struct {
		log *raft.Log
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		index   uint64
		count   uint64
		term    uint64
		ltype   raft.LogType
		data    []byte
		wantErr bool
	}{
		{
			name:  "Store 64 logs",
			index: firstIndex + 1,
			count: 64,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		},
		{
			name:  "Store 65 logs",
			index: firstIndex + 1,
			count: 65,
			term:  uint64(time.Now().Unix() + 3600),
			ltype: raft.LogCommand,
			data:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

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

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if err := s.StoreLogs(logs); (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.StoreLog() error = %v, wantErr %v", err, tt.wantErr)
			}

			for i := uint64(0); i < tt.count; i++ {
				tmp, _ := json.Marshal(logs[i])
				err := check(tt.host, "raft/", fmt.Sprintf("%v%020d", "logs/", tt.index+i), tmp)
				if err != nil {
					t.Errorf("check %v failed: %v", tt.index+i, err)
				}

			}
		})
	}
}

func TestConsulStore_GetLog(t *testing.T) {

	type args struct {
		log *raft.Log
	}
	tests := []struct {
		name    string
		host    string
		prefix  string
		index   uint64
		wantErr bool
	}{
		{
			name:  "Get all test logs ",
			index: 0,
		},
		{
			name:    "Get unknown log",
			index:   1,
			wantErr: true,
		},
	}

	var test = func(s *ConsulStore, index uint64, wantErr bool) {
		log := raft.Log{}
		want := raft.Log{}

		json.Unmarshal(testMessages[index], &want)

		err := s.GetLog(index, &log)

		if (err != nil) != wantErr {
			t.Errorf("ConsulStore.GetLog(%v) error = %v, wantErr %v", index, err, wantErr)
		}

		if !reflect.DeepEqual(log, want) {
			t.Errorf("ConsulStore.GetLog() = %v, want %v", log, want)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			if tt.index == 0 {
				for k := range testMessages {
					test(s, k, tt.wantErr)
				}
			} else {
				test(s, tt.index, tt.wantErr)
			}
		})
	}
}

func TestConsulStore_FirstIndex(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		clear   bool
		want    uint64
		wantErr bool
	}{
		{
			want: firstIndex,
		},
		{
			want:  0,
			clear: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			if tt.clear {
				err := clear(tt.host)
				if err != nil {
					t.Errorf("setup(%v) failed", tt.host)
				}
			} else {
				err := setup(tt.host)
				if err != nil {
					t.Errorf("setup(%v) failed", tt.host)
				}
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			got, err := s.FirstIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.FirstIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConsulStore.FirstIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsulStore_LastIndex(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		want    uint64
		clear   bool
		wantErr bool
	}{
		{
			want: lastIndex,
		},
		{
			want:  0,
			clear: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			if tt.clear {
				err := clear(tt.host)
				if err != nil {
					t.Errorf("setup(%v) failed", tt.host)
				}
			} else {
				err := setup(tt.host)
				if err != nil {
					t.Errorf("setup(%v) failed", tt.host)
				}
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			got, err := s.LastIndex()
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.FirstIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ConsulStore.FirstIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsulStore_DeleteRange(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		prefix  string
		min     uint64
		max     uint64
		wanted  int
		wantErr bool
	}{
		{
			min:    3464435,
			max:    3464435,
			wanted: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Address: tt.host,
				Prefix:  tt.prefix,
			}

			err := setup(tt.host)
			if err != nil {
				t.Errorf("setup(%v) failed", tt.host)
			}

			s, err := NewConsulStore(config)

			if err != nil || s == nil {
				t.Errorf("NewConsulStore(%v) failed", config.Address)
			}

			err = s.DeleteRange(tt.min, tt.max)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConsulStore.FirstIndex() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.prefix == "" {
				tt.prefix = s.prefix
			}

			rest, err := list(tt.host, tt.prefix+"logs/")

			if err != nil {
				t.Errorf("list(%v,%v) failed: %v", tt.host, tt.prefix+"logs/", err)
			}

			want := Uint64Array{}
			for k := range testMessages {
				if k < tt.min || k > tt.max {
					want = append(want, k)
				}
			}

			sort.Sort(want)
			sort.Sort(rest)

			if !reflect.DeepEqual(rest, want) {
				t.Errorf("expected %d logs, got %d", want, rest)
			}

		})
	}
}
